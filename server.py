"""MCP-сервер каталога FARBA для ИИ-агента SendPulse (данные берутся со страниц farba.md).

Транспорт: Streamable HTTP, POST /mcp
Авторизация: Authorization: Bearer <MCP_AUTH_TOKEN>   (в SendPulse: «Bearer-токен»)
Проверка разбора сайта: GET /selftest  (без токена, только публичные данные сайта)
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import re
import time
from typing import Annotated, Any

import anyio
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

from farba_site import CURRENCY, PHONE, STORES, FarbaSite, NotFound, SiteError

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("farba-mcp")

INSTRUCTIONS = f"""\
Каталог магазинов FARBA (ПМР): товары для дома, ремонта, стройки, бытовая техника.
- Цены, наличие и характеристики называй ТОЛЬКО из ответов инструментов.
- Цен две: price.online — при заказе на сайте farba.md (со скидкой), price.in_store — при покупке в магазине.
  Валюта: {CURRENCY}. Если они равны — называй одну цену.
- Наличие по магазинам проверяй с учётом магазина клиента; если он не назван — уточни.
- К каждому товару давай ссылку url.
- Если ok=false — не выдумывай данные, предложи связаться с менеджером (телефон {PHONE}).
"""

server = MCPServer(name="farba-catalog", version="2.0.0", instructions=INSTRUCTIONS)
site = FarbaSite()
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True)
UNAVAILABLE = ("Сайт сейчас не отвечает. Не называй цены и наличие по памяти. "
               f"Извинись и предложи менеджера (телефон {PHONE}).")


def _fail(code: str, hint: str, **extra: Any) -> dict:
    return {"ok": False, "error": code, "hint_for_agent": hint, **extra}


def _public_stores() -> list[dict]:
    return [{k: s[k] for k in ("id", "name", "city", "address")} for s in STORES]


# ---------------------------------------------------------------- магазин из фразы клиента

def _norm(s: str) -> str:
    s = s.lower().replace("ё", "е")
    s = re.sub(r"[^\w\s-]", " ", s)
    s = re.sub(r"\b(в|во|из|на|у|г|город|городе|магазин|магазине|фарба|farba|ул|улица|улице)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


ALIASES = {"bender": "бендер", "bendery": "бендер", "tighina": "бендер", "тигина": "бендер",
           "tiraspol": "тирасп", "ribnita": "рыбниц", "rybnitsa": "рыбниц", "rîbnița": "рыбниц"}


def resolve_store(store: str | None) -> tuple[str | None, dict | None]:
    if not store:
        return None, None
    raw = store.strip()
    if any(s["id"] == raw for s in STORES):
        return raw, None
    q = _norm(raw)
    for k, v in ALIASES.items():
        q = q.replace(k, v)
    # 1) конкретный магазин по ключевому слову (Балка, Горького, Шевченко, гипермаркет)
    exact = [s for s in STORES if any(k in q for k in s["match"])]
    if len(exact) == 1:
        return exact[0]["id"], None
    # 2) город (с учётом падежей: «в Бендерах», «Тирасполе»)
    stem = q[:5]
    by_city = [s for s in STORES if len(stem) >= 4 and _norm(s["city"])[:5] == stem]
    if len(by_city) == 1:
        return by_city[0]["id"], None
    if len(by_city) > 1:
        return None, _fail("ambiguous_store",
                           "В этом городе несколько магазинов FARBA. Уточни у клиента, какой ему удобен, "
                           "или проверь наличие во всех (store не указывай).",
                           candidates=[{k: s[k] for k in ("id", "name", "address")} for s in by_city])
    return None, _fail("unknown_store", "Магазин не найден. Покажи клиенту список магазинов.",
                       stores=_public_stores())


# ---------------------------------------------------------------- helpers

async def _guard(tool: str, coro):
    t0 = time.monotonic()
    try:
        return await coro
    except SiteError as e:
        log.warning("tool=%s site_error=%s %s", tool, e.code, e.detail)
        return _fail(e.code, UNAVAILABLE)
    except NotFound:
        return _fail("not_found", "Товар не найден. Попробуй search_products по названию.")
    finally:
        log.info("tool=%s ms=%d", tool, (time.monotonic() - t0) * 1000)


def _in_range(item: dict, pmin: float | None, pmax: float | None) -> bool:
    p = (item.get("price") or {}).get("online")
    if p is None:
        return pmin is None and pmax is None
    return (pmin is None or p >= pmin) and (pmax is None or p <= pmax)


async def _with_store_stock(items: list[dict], store_id: str, want: int) -> list[dict]:
    """Открывает карточки кандидатов и оставляет только те, что есть в магазине."""
    out: list[dict] = []
    for start in range(0, len(items), 4):
        batch = items[start:start + 4]
        results: list[dict | None] = [None] * len(batch)

        async def load(i: int, it: dict) -> None:
            try:
                results[i] = await site.product(it["url"])
            except (NotFound, SiteError):
                results[i] = None

        async with anyio.create_task_group() as tg:
            for i, it in enumerate(batch):
                tg.start_soon(load, i, it)
        for it, full in zip(batch, results):
            if not full:
                continue
            row = next((s for s in full["stores"] if s.get("store_id") == store_id), None)
            if row and row["qty"] > 0:
                out.append({**it, "price": full["price"] or it.get("price"), "store_stock": row})
        if len(out) >= want:
            break
    return out[:want]


# ================================================================= tools

@server.tool(annotations=READ_ONLY)
async def list_stores() -> dict:
    """Магазины FARBA (id, название, город, адрес). Используй, когда клиент не назвал магазин или назвал неизвестный."""
    return {"ok": True, "stores": _public_stores(), "phone": PHONE}


@server.tool(annotations=READ_ONLY)
async def search_products(
    query: Annotated[str, Field(min_length=2, description="Что ищет клиент: тип товара, бренд, модель или код, например 'шуруповерт аккумуляторный'")],
    price_min: Annotated[float | None, Field(ge=0, description="Минимальная цена, руб.")] = None,
    price_max: Annotated[float | None, Field(ge=0, description="Максимальная цена (бюджет клиента), руб.")] = None,
    store: Annotated[str | None, Field(description="Город или магазин клиента: 'Бендеры', 'Рыбница', 'Балка', 'Горького', 'Шевченко'. Если задан — вернутся только товары, которые есть в этом магазине, с количеством")] = None,
    sort: Annotated[str, Field(description="relevance | price_asc | price_desc")] = "relevance",
    limit: Annotated[int, Field(ge=1, le=8)] = 5,
) -> dict:
    """Поиск товаров на farba.md. Возвращает название, код, цены (на сайте и в магазине), наличие и ссылку.
    Бюджет передавай в price_max. Поиск идёт по словам из названия, поэтому в выдаче бывают аксессуары
    (биты, аккумуляторы) — отбирай подходящее по смыслу."""
    async def run():
        store_id, err = resolve_store(store)
        if err:
            return err
        candidates: list[dict] = []
        page, pages = 1, 1
        need = limit * (3 if store_id else 1)
        while page <= min(pages, 3) and len(candidates) < need:
            items, pages = await site.search(query, sort=sort, page=page, price_min=price_min, price_max=price_max)
            candidates += [i for i in items if _in_range(i, price_min, price_max) and i["availability"] != "out_of_stock"]
            page += 1
        if store_id:
            found = await _with_store_stock(candidates[:12], store_id, limit)
        else:
            found = candidates[:limit]
        res = {"ok": True, "store_id": store_id, "currency": CURRENCY, "items": found}
        if not found:
            res["hint_for_agent"] = ("Подходящих товаров не найдено"
                                     + (" в этом магазине" if store_id else "")
                                     + ". Предложи другой магазин, другой бюджет или другие слова поиска; "
                                       "не говори, что товар есть.")
        return res
    return await _guard("search_products", run())


@server.tool(annotations=READ_ONLY)
async def get_product(
    product: Annotated[str, Field(description="Ссылка на товар farba.md, код товара (ЦБ000525821) или id")],
) -> dict:
    """Полная карточка: цены (на сайте и в магазине), наличие по всем магазинам с количеством, характеристики, описание, ссылка."""
    async def run():
        return {"ok": True, "product": await site.product(product)}
    return await _guard("get_product", run())


@server.tool(annotations=READ_ONLY)
async def check_stock(
    products: Annotated[list[str], Field(min_length=1, max_length=6, description="Ссылки или коды товаров")],
    store: Annotated[str | None, Field(description="Город или магазин. Пусто — все магазины")] = None,
) -> dict:
    """Актуальное количество товаров в магазинах FARBA (status: in_stock | low | out_of_stock)."""
    async def run():
        store_id, err = resolve_store(store)
        if err:
            return err
        out = []
        for ref in products:
            try:
                p = await site.product(ref)
            except NotFound:
                out.append({"product": ref, "error": "not_found"})
                continue
            rows = [s for s in p["stores"] if not store_id or s.get("store_id") == store_id]
            out.append({"name": p["name"], "url": p["url"], "price": p["price"], "stores": rows})
        return {"ok": True, "store_id": store_id, "results": out}
    return await _guard("check_stock", run())


@server.tool(annotations=READ_ONLY)
async def find_alternatives(
    product: Annotated[str, Field(description="Ссылка или код товара, которого нет / который не подошёл")],
    store: Annotated[str | None, Field(description="Город или магазин клиента")] = None,
    price_max: Annotated[float | None, Field(ge=0, description="Бюджет клиента, руб.")] = None,
    limit: Annotated[int, Field(ge=1, le=6)] = 4,
) -> dict:
    """Похожие товары из той же категории, которые есть в наличии (в магазине клиента, если указан)."""
    async def run():
        store_id, err = resolve_store(store)
        if err:
            return err
        base = await site.product(product)
        if not base.get("category"):
            return _fail("no_category", "Категорию определить не удалось — используй search_products.")
        bp = (base.get("price") or {}).get("online")
        pmax = price_max if price_max is not None else (bp * 1.5 if bp else None)
        pmin = bp * 0.5 if (bp and price_max is None) else None
        items: list[dict] = []
        for page in (1, 2):
            got, pages = await site.listing(base["category"]["url"], page=page)
            items += [i for i in got if i["url"] != base["url"] and i["availability"] != "out_of_stock"
                      and _in_range(i, pmin, pmax)]
            if page >= pages or len(items) >= limit * 3:
                break
        if store_id:
            items = await _with_store_stock(items[:12], store_id, limit)
        return {"ok": True, "original": {"name": base["name"], "url": base["url"]},
                "category": base["category"], "store_id": store_id, "items": items[:limit]}
    return await _guard("find_alternatives", run())


# ================================================================= http

@server.custom_route("/health", methods=["GET"])
async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@server.custom_route("/selftest", methods=["GET"])
async def selftest(_: Request) -> JSONResponse:
    """Проверка, что сервер правильно понимает сайт. Открыть в браузере."""
    report: dict[str, Any] = {}
    try:
        items, pages = await site.search("шуруповерт")
        report["search"] = {"found": len(items), "pages": pages, "first": items[:3]}
        sample = os.getenv("SELFTEST_PRODUCT",
                           "https://farba.md/udlinitel-gibkij-dlja-unitaza-s-vypuskom-110mm-30-shtup-tm-santehplast.html")
        p = await site.product(sample)
        report["product"] = p
        checks = {
            "search_finds_products": len(items) > 0,
            "search_has_prices": any(i.get("price") for i in items),
            "product_has_name": bool(p.get("name")),
            "product_has_price": bool(p.get("price")),
            "store_price_looks_sane": bool(p.get("price")) and p["price"]["in_store"] <= p["price"]["online"] * 1.5,
            "search_has_availability": any(i.get("availability") != "unknown" for i in items),
            "product_has_store_stock": any(s.get("qty", 0) > 0 for s in p["stores"]),
            "all_store_rows_recognized": all(s.get("store_id") for s in p["stores"]),
        }
        report["checks"] = checks
        report["status"] = "OK" if all(checks.values()) else "ПРОБЛЕМА — пришлите этот экран в чат"
    except Exception as e:  # noqa: BLE001
        report["status"] = f"ОШИБКА: {type(e).__name__}: {e}"
    return JSONResponse(report, headers={"Cache-Control": "no-store"})


class BearerAuth:
    OPEN = ("/health", "/selftest")

    def __init__(self, app, token: str) -> None:
        self.app, self.token = app, token.encode()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") not in self.OPEN:
            auth = dict(scope.get("headers") or []).get(b"authorization", b"")
            if not (auth.startswith(b"Bearer ") and hmac.compare_digest(auth[7:].strip(), self.token)):
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"application/json"), (b"www-authenticate", b"Bearer")]})
                await send({"type": "http.response.body", "body": json.dumps({"error": "unauthorized"}).encode()})
                return
        await self.app(scope, receive, send)


def build_app():
    token = os.environ.get("MCP_AUTH_TOKEN", "")
    if len(token) < 32:
        raise SystemExit("MCP_AUTH_TOKEN не задан или короче 32 символов")
    app = server.streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True, json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
        host="0.0.0.0",
    )
    return BearerAuth(app, token)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(build_app(), host="0.0.0.0", port=int(os.getenv("PORT", "8080")),
                proxy_headers=True, forwarded_allow_ips="*")
