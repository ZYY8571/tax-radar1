#!/usr/bin/env python3
"""
tax-radar1 -- Self-contained data collector for GitHub Actions.
Scrapes fiscal/tax hot topics from Chinese social-media platforms
and writes JSON files consumed by the GitHub Pages frontend.

Platforms: Weibo, Zhihu, Bilibili (both hot-lists and keyword search).
No API keys required -- uses only public endpoints.

Categories: daily, weekly, monthly, crs, odi, overseas_asset, overseas_company
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("collect")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BJT = timezone(timedelta(hours=8))
NOW = datetime.now(BJT)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Keyword definitions per category
# ---------------------------------------------------------------------------
TAX_KEYWORDS = [
    "增值税", "企业所得税", "个人所得税", "个税", "消费税", "房产税",
    "土地增值税", "印花税", "关税", "车辆购置税", "资源税", "城建税",
    "契税", "环境保护税", "烟叶税", "船舶吨税", "耕地占用税",
    "税收", "税务", "纳税", "退税", "免税", "减税", "避税", "节税",
    "税负", "税率", "税基", "税制", "税改", "税法", "税筹", "税务筹划",
    "发票", "数电发票", "专票", "普票", "进项", "销项",
    "留抵退税", "加计扣除", "即征即退", "先征后退",
    "汇算清缴", "纳税申报", "税务登记", "税收优惠",
    "小规模纳税人", "一般纳税人", "核定征收", "查账征收",
    "财务", "会计", "审计", "财报", "报表", "资产负债", "利润表",
    "现金流", "会计准则", "财务核算", "成本核算", "做账", "记账",
    "应收账款", "应付账款", "固定资产", "折旧", "摊销",
    "CPA", "注册会计师", "税务师", "CMA", "中级会计", "初级会计",
    "财政", "地方财政", "财政收入", "财政支出", "国债", "地方债",
    "转移支付", "预算", "财政部",
    "税务总局", "国家税务", "税务局", "海关总署",
    "报税", "开票", "抵扣", "税前扣除", "专项附加扣除",
    "出口退税", "跨境电商税", "电商税", "直播税",
    "股权转让税", "分红税", "工资税", "年终奖税",
    "社保", "公积金", "五险一金",
]

TAX_PHRASES = [
    "税收政策", "财税新规", "财税改革", "税务合规",
    "企业报税", "个税申报", "增值税发票", "所得税汇算",
    "减税降费", "税收征管", "税务稽查", "税务检查",
    "营商环境", "税收营商", "涉税风险", "税务风险",
]

CRS_KEYWORDS = [
    "CRS申报", "共同申报准则", "税务信息交换", "海外账户申报",
    "AEOI", "金融账户涉税", "CRS合规", "涉税信息交换",
    "非居民金融账户", "尽职调查", "自动交换",
]

ODI_KEYWORDS = [
    "ODI备案", "境外投资备案", "对外直接投资", "境外投资管理",
    "37号文", "返程投资", "海外投资架构", "境外投资审批",
    "对外投资合规", "发改委境外投资",
]

OVERSEAS_ASSET_KEYWORDS = [
    "海外资产申报", "境外资产申报", "海外房产税", "全球征税",
    "海外信托", "境外所得申报", "海外资产配置税务",
    "境外收入纳税", "海外资产合规", "个人境外所得",
]

OVERSEAS_COMPANY_KEYWORDS = [
    "海外公司注册", "离岸公司注册", "香港公司注册", "新加坡公司注册",
    "BVI公司注册", "经济实质法", "离岸架构搭建",
    "开曼公司", "海外公司税务", "注册离岸公司",
]

# Search keywords per category (subset used for active search queries)
CATEGORY_CONFIG = {
    "daily": {
        "keywords": TAX_KEYWORDS,
        "search_terms": ["税收政策", "个人所得税", "增值税", "减税降费", "税务筹划"],
        "filter_keywords": TAX_KEYWORDS,
        "filter_phrases": TAX_PHRASES,
    },
    "weekly": {
        "keywords": TAX_KEYWORDS,
        "search_terms": ["税收政策", "财税改革", "纳税申报"],
        "filter_keywords": TAX_KEYWORDS,
        "filter_phrases": TAX_PHRASES,
    },
    "monthly": {
        "keywords": TAX_KEYWORDS,
        "search_terms": ["税收", "财税新规", "税务"],
        "filter_keywords": TAX_KEYWORDS,
        "filter_phrases": TAX_PHRASES,
    },
    "crs": {
        "keywords": CRS_KEYWORDS,
        "search_terms": ["CRS申报 税务", "共同申报准则", "海外账户涉税申报"],
        "filter_keywords": CRS_KEYWORDS,
        "filter_phrases": [],
    },
    "odi": {
        "keywords": ODI_KEYWORDS,
        "search_terms": ["ODI备案 境外投资", "境外投资备案流程", "海外投资架构 税务"],
        "filter_keywords": ODI_KEYWORDS,
        "filter_phrases": [],
    },
    "overseas_asset": {
        "keywords": OVERSEAS_ASSET_KEYWORDS,
        "search_terms": ["海外资产申报 税务", "境外资产 全球征税", "海外信托 税务"],
        "filter_keywords": OVERSEAS_ASSET_KEYWORDS,
        "filter_phrases": [],
    },
    "overseas_company": {
        "keywords": OVERSEAS_COMPANY_KEYWORDS,
        "search_terms": ["离岸公司注册 税务", "香港公司注册 税务", "海外公司注册"],
        "filter_keywords": OVERSEAS_COMPANY_KEYWORDS,
        "filter_phrases": [],
    },
}

# Platform CSS classes
PLATFORM_WEIBO = "platform-weibo"
PLATFORM_ZHIHU = "platform-zhihu"
PLATFORM_BILIBILI = "platform-bilibili"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_tax_related(text: str, keywords: list[str], phrases: list[str],
                    threshold: int = 1) -> bool:
    """Return True if *text* contains enough matching keywords/phrases."""
    if not text:
        return False
    t = text.lower()
    for phrase in phrases:
        if phrase.lower() in t:
            return True
    hits = 0
    for kw in keywords:
        if kw.lower() in t:
            hits += 1
            if hits >= threshold:
                return True
    return False


def extract_tags(text: str, keywords: list[str], limit: int = 3) -> list[str]:
    """Pull matching keywords from text as tags."""
    tags: list[str] = []
    for kw in keywords:
        if kw in text and len(kw) >= 2:
            tags.append(kw)
        if len(tags) >= limit:
            break
    return tags


def format_discussions(n: int) -> str:
    if n >= 10000:
        return f"{n / 10000:.1f}万"
    return str(n)


def make_id(source: str, title: str) -> str:
    return hashlib.md5(f"{source}:{title}".encode()).hexdigest()[:12]


def make_detail(title: str, summary: str, points: list[str] | None = None) -> str:
    """Build a short HTML detail block."""
    html = f"<p>{summary}</p>"
    if points:
        html += "<h4>要点</h4><ul>" + "".join(f"<li>{p}</li>" for p in points) + "</ul>"
    return html


def topic_dict(
    title: str, summary: str, source: str, platform_class: str,
    author: str, url: str, heat: int, discussions, tags: list[str],
    is_hot: bool, detail: str | None = None,
) -> dict:
    return {
        "id": make_id(source, title),
        "title": title,
        "summary": summary,
        "source": source,
        "platform_class": platform_class,
        "author": author,
        "url": url,
        "time": NOW.strftime("%Y-%m-%d %H:%M"),
        "heat": int(heat),
        "discussions": discussions if isinstance(discussions, str) else format_discussions(discussions),
        "tags": tags,
        "isHot": is_hot,
        "detail": detail or make_detail(title, summary),
    }


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=30.0,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept": "application/json, text/html, */*",
        },
        follow_redirects=True,
    )


async def random_delay(lo: float = 0.5, hi: float = 2.0) -> None:
    """Sleep a random duration to avoid rate limiting."""
    await asyncio.sleep(random.uniform(lo, hi))


# ---------------------------------------------------------------------------
# Weibo collectors
# ---------------------------------------------------------------------------
# PLACEHOLDER: weibo_hotlist
async def collect_weibo_hotlist(keywords: list[str], phrases: list[str]) -> list[dict]:
    """Weibo hot-search: filter for matching keywords."""
    url = "https://weibo.com/ajax/side/hotSearch"
    results: list[dict] = []
    try:
        async with make_client() as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Weibo hotlist returned HTTP %s", resp.status_code)
                return results
            data = resp.json()
            for item in data.get("data", {}).get("realtime", []):
                word = item.get("word", "")
                note = item.get("note", "")
                raw_heat = item.get("raw_hot", 0) or item.get("num", 0)
                combined = word + " " + note
                if not is_tax_related(combined, keywords, phrases):
                    continue
                results.append(topic_dict(
                    title=word,
                    summary=note or word,
                    source="微博",
                    platform_class=PLATFORM_WEIBO,
                    author="微博热搜",
                    url=f"https://s.weibo.com/weibo?q=%23{quote(word)}%23",
                    heat=raw_heat,
                    discussions=format_discussions(raw_heat),
                    tags=extract_tags(combined, keywords),
                    is_hot=raw_heat > 500000,
                ))
    except Exception as e:
        logger.warning("Weibo hotlist collection failed: %s", e)
    return results

# PLACEHOLDER: weibo_search
async def collect_weibo_search(search_terms: list[str], keywords: list[str],
                                phrases: list[str]) -> list[dict]:
    """Search Weibo for specific keywords."""
    results: list[dict] = []
    try:
        async with make_client() as client:
            for term in search_terms:
                await random_delay(1.0, 3.0)
                encoded = quote(term)
                url = (
                    f"https://m.weibo.cn/api/container/getIndex"
                    f"?containerid=100103type%3D1%26q%3D{encoded}"
                )
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        logger.warning("Weibo search '%s' returned HTTP %s", term, resp.status_code)
                        continue
                    data = resp.json()
                    cards = data.get("data", {}).get("cards", [])
                    for card in cards:
                        card_group = card.get("card_group", [])
                        if not card_group:
                            # Single card (not grouped)
                            card_group = [card]
                        for cg in card_group:
                            mblog = cg.get("mblog")
                            if not mblog:
                                continue
                            text_raw = mblog.get("text", "")
                            # Strip HTML tags for plain text
                            text_plain = re.sub(r"<[^>]+>", "", text_raw)
                            title = text_plain[:80].strip()
                            if not title:
                                continue
                            reposts = mblog.get("reposts_count", 0)
                            comments = mblog.get("comments_count", 0)
                            attitudes = mblog.get("attitudes_count", 0)
                            heat = int(attitudes * 2 + comments * 5 + reposts * 3)
                            user_info = mblog.get("user", {}) or {}
                            mid = mblog.get("mid", "") or mblog.get("id", "")
                            results.append(topic_dict(
                                title=title,
                                summary=text_plain[:200].strip(),
                                source="微博",
                                platform_class=PLATFORM_WEIBO,
                                author=user_info.get("screen_name", ""),
                                url=f"https://m.weibo.cn/detail/{mid}",
                                heat=heat,
                                discussions=format_discussions(comments),
                                tags=extract_tags(text_plain, keywords),
                                is_hot=attitudes > 10000,
                            ))
                except Exception as e:
                    logger.warning("Weibo search '%s' failed: %s", term, e)
    except Exception as e:
        logger.warning("Weibo search collection failed: %s", e)
    return results


# ---------------------------------------------------------------------------
# Zhihu collectors
# ---------------------------------------------------------------------------
# PLACEHOLDER: zhihu_hotlist
async def collect_zhihu_hotlist(keywords: list[str], phrases: list[str]) -> list[dict]:
    """Zhihu hot-list: filter for matching keywords."""
    url = "https://www.zhihu.com/api/v3/feed/topstory/hot-lists/total"
    results: list[dict] = []
    try:
        async with make_client() as client:
            resp = await client.get(url, params={"limit": 50})
            if resp.status_code != 200:
                logger.warning("Zhihu hotlist returned HTTP %s", resp.status_code)
                return results
            data = resp.json()
            for item in data.get("data", []):
                target = item.get("target", {})
                title = target.get("title", "")
                excerpt = target.get("excerpt", "")
                combined = title + " " + excerpt
                if not is_tax_related(combined, keywords, phrases):
                    continue
                detail_text = item.get("detail_text", "0")
                heat_num = int(re.sub(r"[^\d]", "", detail_text) or "0")
                answer_count = target.get("answer_count", 0)
                qid = target.get("id", "")
                results.append(topic_dict(
                    title=title,
                    summary=excerpt[:200] if excerpt else title,
                    source="知乎",
                    platform_class=PLATFORM_ZHIHU,
                    author=target.get("author", {}).get("name", ""),
                    url=f"https://www.zhihu.com/question/{qid}",
                    heat=heat_num,
                    discussions=format_discussions(answer_count),
                    tags=extract_tags(combined, keywords),
                    is_hot=heat_num > 1000000,
                ))
    except Exception as e:
        logger.warning("Zhihu hotlist collection failed: %s", e)
    return results

# PLACEHOLDER: zhihu_search
async def collect_zhihu_search(search_terms: list[str], keywords: list[str],
                                phrases: list[str]) -> list[dict]:
    """Search Zhihu for specific keywords."""
    results: list[dict] = []
    try:
        async with make_client() as client:
            for term in search_terms:
                await random_delay(1.0, 3.0)
                url = "https://www.zhihu.com/api/v4/search_v3"
                try:
                    resp = await client.get(url, params={"t": "general", "q": term})
                    if resp.status_code != 200:
                        logger.warning("Zhihu search '%s' returned HTTP %s", term, resp.status_code)
                        continue
                    data = resp.json()
                    for item in data.get("data", []):
                        obj = item.get("object", {})
                        item_type = item.get("type", "")
                        title = ""
                        excerpt = ""
                        author_name = ""
                        qid = ""
                        answer_count = 0
                        follower_count = 0
                        result_url = ""

                        if item_type == "search_result":
                            # Could be question, answer, or article
                            title = obj.get("title", "") or ""
                            # Strip HTML highlight tags
                            title = re.sub(r"<[^>]+>", "", title)
                            excerpt = obj.get("excerpt", "") or obj.get("description", "") or ""
                            excerpt = re.sub(r"<[^>]+>", "", excerpt)
                            author_info = obj.get("author", {}) or {}
                            author_name = author_info.get("name", "")
                            question = obj.get("question", {}) or {}
                            qid = question.get("id", "") or obj.get("id", "")
                            answer_count = question.get("answer_count", 0) or 0
                            follower_count = question.get("follower_count", 0) or 0
                            obj_type = obj.get("type", "")
                            if obj_type == "answer":
                                result_url = f"https://www.zhihu.com/question/{question.get('id', '')}/answer/{obj.get('id', '')}"
                            elif obj_type == "article":
                                result_url = f"https://zhuanlan.zhihu.com/p/{obj.get('id', '')}"
                            else:
                                result_url = f"https://www.zhihu.com/question/{qid}"
                        else:
                            continue

                        if not title:
                            continue

                        heat = int(follower_count * 0.5 + answer_count * 10)
                        results.append(topic_dict(
                            title=title,
                            summary=excerpt[:200] if excerpt else title,
                            source="知乎",
                            platform_class=PLATFORM_ZHIHU,
                            author=author_name,
                            url=result_url or f"https://www.zhihu.com/search?type=content&q={quote(term)}",
                            heat=heat,
                            discussions=format_discussions(answer_count),
                            tags=extract_tags(title + " " + excerpt, keywords),
                            is_hot=follower_count > 5000,
                        ))
                except Exception as e:
                    logger.warning("Zhihu search '%s' failed: %s", term, e)
    except Exception as e:
        logger.warning("Zhihu search collection failed: %s", e)
    return results


# ---------------------------------------------------------------------------
# Bilibili collectors
# ---------------------------------------------------------------------------
# PLACEHOLDER: bilibili_ranking
async def collect_bilibili_ranking(keywords: list[str], phrases: list[str]) -> list[dict]:
    """Bilibili ranking (all categories): filter for matching keywords."""
    url = "https://api.bilibili.com/x/web-interface/ranking/v2"
    results: list[dict] = []
    try:
        async with make_client() as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Bilibili ranking returned HTTP %s", resp.status_code)
                return results
            data = resp.json()
            for item in data.get("data", {}).get("list", []):
                title = item.get("title", "")
                desc = item.get("desc", "")
                combined = title + " " + desc
                if not is_tax_related(combined, keywords, phrases):
                    continue
                stat = item.get("stat", {})
                view = stat.get("view", 0)
                danmaku = stat.get("danmaku", 0)
                like = stat.get("like", 0)
                reply = stat.get("reply", 0)
                heat = int(view * 0.1 + like * 2 + reply * 5 + danmaku * 1)
                bvid = item.get("bvid", "")
                results.append(topic_dict(
                    title=title,
                    summary=desc[:200] if desc else title,
                    source="B站",
                    platform_class=PLATFORM_BILIBILI,
                    author=item.get("owner", {}).get("name", ""),
                    url=f"https://www.bilibili.com/video/{bvid}" if bvid else "",
                    heat=heat,
                    discussions=format_discussions(danmaku + reply),
                    tags=extract_tags(combined, keywords),
                    is_hot=view > 500000,
                ))
    except Exception as e:
        logger.warning("Bilibili ranking collection failed: %s", e)
    return results

# PLACEHOLDER: bilibili_search
async def collect_bilibili_search(search_terms: list[str], keywords: list[str],
                                   phrases: list[str]) -> list[dict]:
    """Search Bilibili for specific keywords."""
    results: list[dict] = []
    try:
        async with make_client() as client:
            for term in search_terms:
                await random_delay(1.0, 3.0)
                url = "https://api.bilibili.com/x/web-interface/search/all/v2"
                try:
                    resp = await client.get(url, params={"keyword": term})
                    if resp.status_code != 200:
                        logger.warning("Bilibili search '%s' returned HTTP %s", term, resp.status_code)
                        continue
                    data = resp.json()
                    result_list = data.get("data", {}).get("result", [])
                    for group in result_list:
                        # Each group has a "result_type" and "data" list
                        if group.get("result_type") != "video":
                            continue
                        for item in group.get("data", []):
                            title = item.get("title", "")
                            # Strip HTML highlight tags from search results
                            title = re.sub(r"<[^>]+>", "", title)
                            desc = item.get("description", "")
                            if not title:
                                continue
                            view = item.get("play", 0) or 0
                            danmaku = item.get("danmaku", 0) or 0
                            like = item.get("like", 0) or 0
                            review = item.get("review", 0) or 0
                            # 'play' may be a string like "1.2万"
                            if isinstance(view, str):
                                view = _parse_chinese_num(view)
                            heat = int(view * 0.1 + like * 2 + review * 5 + danmaku * 1)
                            bvid = item.get("bvid", "")
                            arcurl = item.get("arcurl", "")
                            results.append(topic_dict(
                                title=title,
                                summary=desc[:200] if desc else title,
                                source="B站",
                                platform_class=PLATFORM_BILIBILI,
                                author=item.get("author", ""),
                                url=arcurl or (f"https://www.bilibili.com/video/{bvid}" if bvid else ""),
                                heat=heat,
                                discussions=format_discussions(danmaku + review),
                                tags=extract_tags(title + " " + desc, keywords),
                                is_hot=view > 100000,
                            ))
                except Exception as e:
                    logger.warning("Bilibili search '%s' failed: %s", term, e)
    except Exception as e:
        logger.warning("Bilibili search collection failed: %s", e)
    return results


def _parse_chinese_num(s: str) -> int:
    """Parse strings like '1.2万' into integers."""
    s = s.strip()
    if not s:
        return 0
    try:
        if "万" in s:
            return int(float(s.replace("万", "")) * 10000)
        if "亿" in s:
            return int(float(s.replace("亿", "")) * 100000000)
        return int(float(re.sub(r"[^\d.]", "", s)))
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# Category collector
# ---------------------------------------------------------------------------
# PLACEHOLDER: collect_category
async def collect_category(category_name: str) -> list[dict]:
    """Collect topics for a single category from all platforms."""
    config = CATEGORY_CONFIG[category_name]
    kw = config["filter_keywords"]
    phrases = config["filter_phrases"]
    search_terms = config["search_terms"]

    logger.info("Collecting category '%s' with %d search terms...", category_name, len(search_terms))

    # Run search-based collectors concurrently (more reliable from non-China IPs)
    search_tasks = [
        collect_weibo_search(search_terms, kw, phrases),
        collect_zhihu_search(search_terms, kw, phrases),
        collect_bilibili_search(search_terms, kw, phrases),
    ]

    # Also run hotlist/ranking collectors as fallback (only for daily/weekly/monthly)
    hotlist_tasks: list = []
    if category_name in ("daily", "weekly", "monthly"):
        hotlist_tasks = [
            collect_weibo_hotlist(kw, phrases),
            collect_zhihu_hotlist(kw, phrases),
            collect_bilibili_ranking(kw, phrases),
        ]

    all_tasks = search_tasks + hotlist_tasks
    task_labels = [
        "Weibo-search", "Zhihu-search", "Bilibili-search",
    ] + (["Weibo-hotlist", "Zhihu-hotlist", "Bilibili-ranking"] if hotlist_tasks else [])

    results_raw = await asyncio.gather(*all_tasks, return_exceptions=True)

    all_items: list[dict] = []
    for label, result in zip(task_labels, results_raw):
        if isinstance(result, Exception):
            logger.error("[%s] %s raised: %s", category_name, label, result)
            continue
        logger.info("[%s] %s returned %d items", category_name, label, len(result))
        all_items.extend(result)

    # Deduplicate by title (normalized)
    seen: set[str] = set()
    unique: list[dict] = []
    for item in all_items:
        norm_title = item["title"].strip().lower()
        if norm_title not in seen:
            seen.add(norm_title)
            unique.append(item)

    # Sort by heat descending
    unique.sort(key=lambda x: x["heat"], reverse=True)

    # Mark top 3 as hot
    for i, item in enumerate(unique):
        item["isHot"] = i < 3

    logger.info("[%s] %d unique topics after dedup", category_name, len(unique))
    return unique


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
# PLACEHOLDER: collect_all
async def collect_all() -> dict[str, list[dict]]:
    """Collect all categories. Returns {category_name: [topics]}."""
    results: dict[str, list[dict]] = {}
    # Collect categories sequentially to be gentle on rate limits.
    # daily/weekly/monthly share the same sources so collect once and reuse.
    daily_topics = await collect_category("daily")
    results["daily"] = daily_topics

    # Weekly and monthly reuse daily data (same keywords, different file paths)
    # but we re-collect with their own (smaller) search term sets for variety.
    for cat in ("weekly", "monthly"):
        await random_delay(2.0, 4.0)
        results[cat] = await collect_category(cat)

    # Specialty categories
    for cat in ("crs", "odi", "overseas_asset", "overseas_company"):
        await random_delay(2.0, 4.0)
        results[cat] = await collect_category(cat)

    return results


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------
# PLACEHOLDER: save_json
def save_all(results: dict[str, list[dict]]) -> None:
    """Write JSON files for every category, always creating directories."""
    base = Path(os.getenv("DATA_DIR", "data"))
    now = NOW

    date_str = now.strftime("%Y-%m-%d")
    iso_cal = now.isocalendar()
    week_str = f"{iso_cal[0]}-W{iso_cal[1]:02d}"
    month_str = now.strftime("%Y-%m")

    # Map category -> (subdirectory, filename)
    file_map: dict[str, tuple[str, str]] = {
        "daily": ("daily", f"{date_str}.json"),
        "weekly": ("weekly", f"{week_str}.json"),
        "monthly": ("monthly", f"{month_str}.json"),
        "crs": ("crs", "latest.json"),
        "odi": ("odi", "latest.json"),
        "overseas_asset": ("overseas_asset", "latest.json"),
        "overseas_company": ("overseas_company", "latest.json"),
    }

    # Always create all directories, even if no topics
    for subdir, _ in file_map.values():
        (base / subdir).mkdir(parents=True, exist_ok=True)

    for category, (subdir, filename) in file_map.items():
        topics = results.get(category, [])
        payload = {
            "updated_at": now.isoformat(),
            "count": len(topics),
            "topics": topics,
        }
        path = base / subdir / filename
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Wrote %s  (%d topics)", path, len(topics))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    logger.info("=== tax-radar collector started at %s ===", NOW.strftime("%Y-%m-%d %H:%M %Z"))
    results = await collect_all()
    save_all(results)
    total = sum(len(v) for v in results.values())
    logger.info("Collection complete: %d total topics across %d categories.", total, len(results))


if __name__ == "__main__":
    asyncio.run(main())
