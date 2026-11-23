"""Чтение публичных страниц farba.md: поиск, категория, карточка товара.

Сайт отдаёт всё в HTML (цены, наличие по магазинам), поэтому разбираем текст страниц
по устойчивым признакам: «Код: ЦБ…», «… Руб», «28шт. - магазин …», <h1>, <h4>.
Если вёрстка сайта изменится — правится только этот файл.
"""

from __future__ import annotations

import html as htmllib
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import anyio
import anyio.to_thread

log = logging.getLogger("farba-mcp")

SITE = "https://farba.md"
BASE = os.getenv("FARBA_BASE_URL", SITE).rstrip("/")  # менять только для тестов
CURRENCY = "руб. ПМР"

# Магазины — как они подписаны на сайте в блоке «Самовывоз из».
# match: кусочки текста, по которым строка на сайте относится к магазину.
STORES: list[dict[str, Any]] = [
    {"id": "tir-shevchenko", "name": "Гипермаркет «Фарба» на Шевченко", "city": "Тирасполь",
     "address": "ул. Шевченко, 66/1", "match": ["шевченк", "гипермаркет"]},
    {"id": "tir-gorkogo", "name": "Магазин «Фарба» на Горького", "city": "Тирасполь",
     "address": "ул. Горького, 56", "match": ["горьк"]},
    {"id": "tir-balka", "name": "Магазин на Балке", "city": "Тирасполь",
     "address": "ул. Краснодонская, 58", "match": ["балк", "краснодон"]},
    {"id": "bnd", "name": "Магазин «Фарба»", "city": "Бендеры",
     "address": "ул. Ермакова, 1/1", "match": ["бендер", "ермаков"]},
    {"id": "rbn", "name": "Магазин «Фарба» на Кирова", "city": "Рыбница",
     "address": "ул. Кирова, 110/2", "match": ["рыбниц", "кирова"]},
]
PHONE = "15-05"


class SiteError(Exception):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}")
        self.code, self.detail = code, detail


class NotFound(Exception):
    pass


# ======================================================================== text utils

_BLOCK = re.compile(r"<\s*(br|/li|/div|/p|/h\d|/tr|/ul|/ol|li|h\d|p|div)\b[^>]*>", re.I)
_TAG = re.compile(r"<[^>]+>")
# Тысячи бывают через пробел («1 499,95»), но соседние числа склеивать нельзя
# (на карточке код «ЦБ000525821» стоит прямо перед ценой «72,95 Руб»).
_PRICE = re.compile(r"(?<![\d,.])((?:\d{1,3}(?:[ \u00a0]\d{3})+|\d+),\d{2})\s*Руб")


def text_lines(fragment: str) -> list[str]:
    fragment = re.sub(r"<(script|style)\b.*?</\1>", " ", fragment, flags=re.S | re.I)
    t = _BLOCK.sub("\n", fragment)
    t = htmllib.unescape(_TAG.sub(" ", t))
    lines = [re.sub(r"[ \t ]+", " ", ln).strip() for ln in t.split("\n")]
    return [ln for ln in lines if ln]


def plain(fragment: str) -> str:
    return " ".join(text_lines(fragment))


def to_num(s: str) -> float:
    return float(re.sub(r"[\s ]", "", s).replace(",", "."))


def prices_in(fragment: str) -> list[float]:
    # «Кешбэк 1,40 Руб» — не цена товара
    txt = plain(fragment)
    txt = re.sub(r"Кешб[эе]к\s*\d[\d\s]*,\d{2}\s*Руб", " ", txt)
    return [to_num(m) for m in _PRICE.findall(txt)]


def price_obj(prices: list[float]) -> dict | None:
    """На сайте: зачёркнутая цена = цена в магазине, вторая = цена при заказе на сайте."""
    if not prices:
        return None
    if len(prices) >= 2 and prices[1] < prices[0] <= prices[1] * 1.5:  # скидка сайта — несколько процентов
        return {"online": prices[1], "in_store": prices[0], "currency": CURRENCY}
    return {"online": prices[0], "in_store": prices[0], "currency": CURRENCY}


def abs_url(href: str) -> str:
    href = htmllib.unescape(href.strip())
    return urllib.parse.urljoin(SITE + "/", href)


def is_site_url(url: str) -> bool:
    p = urllib.parse.urlparse(url)
    allowed = {"farba.md", "www.farba.md", urllib.parse.urlparse(BASE).hostname}
    return p.scheme in ("http", "https") and p.hostname in allowed


def match_store(label: str) -> dict | None:
    low = label.lower().replace("ё", "е")
    for s in STORES:
        if any(k in low for k in s["match"]):
            return s
    return None


# ======================================================================== parsers

_CARD_SPLIT = re.compile(r"wishlist_now(?:&amp;|&)BUYproducts_id=(\d+)")


def parse_listing(page_html: str) -> list[dict]:
    """Карточки товаров на странице поиска / категории."""
    marks = list(_CARD_SPLIT.finditer(page_html))
    items, seen = [], set()
    for i, m in enumerate(marks):
        pid = m.group(1)
        if pid in seen:
            continue
        end = marks[i + 1].start() if i + 1 < len(marks) else len(page_html)
        seg = page_html[m.end(): end]
        h4 = re.search(r"<h4[^>]*>(.*?)</h4>", seg, re.S | re.I)
        if not h4:
            continue
        link = re.search(r'href="([^"]+\.html[^"]*)"', h4.group(1)) or re.search(r'href="([^"]+\.html[^"]*)"', seg)
        name = plain(h4.group(1))
        code = re.search(r"Код:\s*([A-Za-zА-Яа-я]{0,4}\d{4,})", plain(seg))
        before = plain(page_html[max(0, m.start() - 2500): m.start()])
        after_h4 = seg[h4.end():]
        avail = "unknown"
        # бейдж наличия стоит перед «В избранное» (или внутри карточки до названия)
        tail = (before[-250:].split("руб./мес")[-1] + " " + plain(seg[: h4.start()])).lower()
        if "нет в наличии" in tail:
            avail = "out_of_stock"
        elif "под заказ" in tail:
            avail = "on_order"
        elif "в наличии" in tail:
            avail = "in_stock"
        if not link or not name:
            continue
        seen.add(pid)
        items.append({
            "id": pid,
            "code": code.group(1) if code else None,
            "name": name,
            "url": abs_url(link.group(1)),
            "price": price_obj(prices_in(after_h4.split("Рассрочка")[0])),
            "availability": avail,
        })
    return items


def parse_total_pages(page_html: str) -> int:
    m = re.search(r"\b(\d+)\s*/\s*(\d+)\b", plain(page_html.split("Сортировка", 1)[-1][:3000]))
    return int(m.group(2)) if m else 1


def parse_product(page_html: str, url: str) -> dict:
    h1m = re.search(r"<h1[^>]*>(.*?)</h1>", page_html, re.S | re.I)
    if not h1m:
        raise NotFound(url)
    name = plain(h1m.group(1))
    head, body = page_html[: h1m.start()], page_html[h1m.end():]

    # категория — последняя ссылка .html в «хлебных крошках» перед <h1>
    crumbs = re.findall(r'<a[^>]+href="([^"]+\.html)"[^>]*>(.*?)</a>', head[-6000:], re.S | re.I)
    category = None
    for href, label in reversed(crumbs):
        lbl = plain(label)
        if lbl and lbl.lower() not in ("главная",):
            category = {"name": lbl, "url": abs_url(href)}
            break

    # основной блок: от <h1> до «Самовывоз из» (или до «Доставка»)
    stop = re.search(r"Самовывоз из|Доставка", body)
    main = body[: stop.start()] if stop else body[:15000]
    main_txt = plain(main)
    price_zone = re.split(r"за\s+шт|Цена действует|В корзину", main, maxsplit=1)[0]
    price = price_obj(prices_in(price_zone))
    unit = (re.search(r"за\s+(шт|м2|м²|м|кг|л|уп|компл)\.?", main_txt) or [None, None])[1]
    code = re.search(r"Код:\s*([A-Za-zА-Яа-я]{0,4}\d{4,})", main_txt)
    pid = re.search(r"BUYproducts_id=(\d+)", main)
    low = main_txt.lower()
    availability = ("out_of_stock" if "нет в наличии" in low else
                    "on_order" if "под заказ" in low else
                    "in_stock" if "в наличии" in low else "unknown")
    img = re.search(r'href="([^"]*product_images/popup_images/[^"]+)"', main) or \
        re.search(r'src="([^"]*product_images/[^"]+)"', main)

    # наличие по магазинам
    stock_rows: dict[str, dict] = {}
    other_rows = []
    sm = re.search(r"Самовывоз из:?(.*?)(?:Доставка|Оплата|Описание|<h2)", body, re.S | re.I)
    if sm:
        for ln in text_lines(sm.group(1)):
            mm = re.match(r"(\d+)\s*шт\.?\s*[-–—]\s*(.+)", ln)
            if not mm:
                continue
            qty, label = int(mm.group(1)), mm.group(2).strip()
            st = match_store(label)
            if st:
                stock_rows[st["id"]] = {"store_id": st["id"], "store_name": f'{st["name"]}, {st["city"]}',
                                        "qty": qty, "status": _status(qty)}
            else:
                other_rows.append({"store_name": label, "qty": qty, "status": _status(qty)})
    stores = []
    for st in STORES:  # магазина нет в списке на сайте = 0 шт.
        stores.append(stock_rows.get(st["id"]) or {
            "store_id": st["id"], "store_name": f'{st["name"]}, {st["city"]}', "qty": 0, "status": "out_of_stock"})
    stores += other_rows

    # описание и характеристики
    desc, attrs = None, {}
    dm = re.search(r"<h2[^>]*>\s*Описание.*?</h2>(.*?)(?:<h2|<h3)", body, re.S | re.I)
    if dm:
        desc = plain(dm.group(1))[:500] or None
    am = re.search(r"<h2[^>]*>\s*Характеристики.*?</h2>(.*?)(?:<h2|<h3|Отзывы покупателей)", body, re.S | re.I)
    if am:
        for ln in text_lines(am.group(1)):
            if ":" in ln:
                k, v = ln.split(":", 1)
                if k.strip() and v.strip() and len(k) < 60:
                    attrs[k.strip()] = v.strip()[:200]

    return {
        "id": pid.group(1) if pid else None,
        "code": code.group(1) if code else None,
        "name": name,
        "url": url,
        "image": abs_url(img.group(1)) if img else None,
        "category": category,
        "price": price,
        "unit": unit,
        "availability": availability,
        "stores": stores,
        "attributes": attrs,
        "description": desc,
    }


def _status(q: int) -> str:
    return "out_of_stock" if q <= 0 else ("low" if q <= 2 else "in_stock")


# ======================================================================== http client

class TTLCache:
    def __init__(self, max_items: int = 3000) -> None:
        self._d: dict[str, tuple[float, Any]] = {}
        self._max = max_items

    def get(self, k: str):
        v = self._d.get(k)
        if not v or v[0] < time.monotonic():
            self._d.pop(k, None)
            return None
        return v[1]

    def set(self, k: str, val: Any, ttl: float) -> None:
        if len(self._d) >= self._max:
            for key, _ in sorted(self._d.items(), key=lambda kv: kv[1][0])[: self._max // 10]:
                self._d.pop(key, None)
        self._d[k] = (time.monotonic() + ttl, val)


class FarbaSite:
    def __init__(self) -> None:
        self.timeout = float(os.getenv("SITE_TIMEOUT", "8"))
        self.min_interval = 1.0 / float(os.getenv("SITE_RPS", "3"))
        self.ttl_list = float(os.getenv("CACHE_TTL_LIST", "300"))
        self.ttl_product = float(os.getenv("CACHE_TTL_PRODUCT", "120"))
        self.cache = TTLCache()
        self.sem = anyio.Semaphore(int(os.getenv("SITE_CONCURRENCY", "4")))
        self._last = 0.0
        self._lock = anyio.Lock()

    def _fetch_sync(self, url: str) -> str:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FarbaAIConsultant/1.0; +https://farba.md)",
            "Accept": "text/html", "Accept-Language": "ru",
        })
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                raw = r.read()
                charset = r.headers.get_content_charset() or "utf-8"
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NotFound(url) from e
            raise SiteError("site_error", f"HTTP {e.code}") from e
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise SiteError("site_unavailable", str(e)) from e
        try:
            return raw.decode(charset, errors="replace")
        except LookupError:
            return raw.decode("utf-8", errors="replace")

    async def get(self, url: str, ttl: float) -> str:
        if not is_site_url(url):
            raise SiteError("bad_url", url)
        if BASE != SITE:  # тестовый режим: ходим в подставной сайт
            url = re.sub(r"^https?://(www\.)?farba\.md", BASE, url)
        hit = self.cache.get(url)
        if hit is not None:
            return hit
        async with self.sem:
            async with self._lock:  # не чаще SITE_RPS запросов в секунду
                wait = self._last + self.min_interval - time.monotonic()
                if wait > 0:
                    await anyio.sleep(wait)
                self._last = time.monotonic()
            page = await anyio.to_thread.run_sync(self._fetch_sync, url)
        if re.search(r"\d,\d{2}\s*(лей|Лей|MDL)", page) and "Руб" not in page:
            log.warning("сайт отдал цены не в рублях: %s", url)
        self.cache.set(url, page, ttl)
        return page

    # -- high level
    async def search(self, query: str, *, sort: str = "relevance", page: int = 1,
                     price_min: float | None = None, price_max: float | None = None) -> tuple[list[dict], int]:
        q: dict[str, Any] = {"keywords": query}
        if sort == "price_asc":
            q.update(sort="price", direction="asc")
        elif sort == "price_desc":
            q.update(sort="price", direction="desc")
        if price_min is not None:
            q["pfrom"] = int(price_min)
        if price_max is not None:
            q["pto"] = int(price_max + 0.99)
        if page > 1:
            q["page"] = page
        url = f"{SITE}/advanced_search_result.php?" + urllib.parse.urlencode(q)
        page_html = await self.get(url, self.ttl_list)
        return parse_listing(page_html), parse_total_pages(page_html)

    async def listing(self, url: str, *, sort: str = "relevance", page: int = 1) -> tuple[list[dict], int]:
        q: dict[str, Any] = {}
        if sort == "price_asc":
            q.update(sort="price", direction="asc")
        elif sort == "price_desc":
            q.update(sort="price", direction="desc")
        if page > 1:
            q["page"] = page
        full = url + (("&" if "?" in url else "?") + urllib.parse.urlencode(q) if q else "")
        page_html = await self.get(full, self.ttl_list)
        return parse_listing(page_html), parse_total_pages(page_html)

    async def product(self, ref: str) -> dict:
        ref = ref.strip()
        if ref.startswith("http"):
            url = ref
        elif re.fullmatch(r"\d{3,}", ref):
            url = f"{SITE}/product_info.php?products_id={ref}"
        elif re.fullmatch(r"[A-Za-zА-Яа-я]{1,4}\d{4,}", ref):  # код ЦБ000525821
            items, _ = await self.search(ref)
            hit = next((i for i in items if (i.get("code") or "").lower() == ref.lower()), None)
            if not hit:
                raise NotFound(ref)
            url = hit["url"]
        else:
            url = f"{SITE}/{ref.lstrip('/')}"
        page_html = await self.get(url, self.ttl_product)
        return parse_product(page_html, url)
