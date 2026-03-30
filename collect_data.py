#!/usr/bin/env python3
"""
tax-radar1 -- Self-contained data collector for GitHub Actions.
Scrapes fiscal/tax hot topics from Chinese social-media platforms
and writes JSON files consumed by the GitHub Pages frontend.

Platforms: Weibo hot-search, Zhihu hot-list, Bilibili ranking.
No API keys required -- uses only public endpoints.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

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

# --- Tax keyword lists (inline, no external imports) ---
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

# Platform CSS classes
PLATFORM_WEIBO = "platform-weibo"
PLATFORM_ZHIHU = "platform-zhihu"
PLATFORM_BILIBILI = "platform-bilibili"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_tax_related(text: str, threshold: int = 1) -> bool:
    """Return True if *text* contains at least *threshold* tax keywords."""
    if not text:
        return False
    t = text.lower()
    for phrase in TAX_PHRASES:
        if phrase in t:
            return True
    hits = 0
    for kw in TAX_KEYWORDS:
        if kw in t:
            hits += 1
            if hits >= threshold:
                return True
    return False


def extract_tags(text: str, limit: int = 3) -> list[str]:
    tags = []
    for kw in TAX_KEYWORDS[:40]:
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
    title, summary, source, platform_class, author, url,
    heat, discussions, tags, is_hot, detail=None,
):
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
        },
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------

# placeholder -- each collector is filled in via Edit below
async def collect_weibo() -> list[dict]:
    """Weibo hot-search: filter tax-related entries."""
    url = "https://weibo.com/ajax/side/hotSearch"
    results = []
    try:
        async with make_client() as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Weibo returned HTTP %s", resp.status_code)
                return results
            data = resp.json()
            for item in data.get("data", {}).get("realtime", []):
                word = item.get("word", "")
                note = item.get("note", "")
                raw_heat = item.get("raw_hot", 0) or item.get("num", 0)
                if not is_tax_related(word + " " + note):
                    continue
                results.append(topic_dict(
                    title=word,
                    summary=note or word,
                    source="微博",
                    platform_class=PLATFORM_WEIBO,
                    author="微博热搜",
                    url=f"https://s.weibo.com/weibo?q=%23{word}%23",
                    heat=raw_heat,
                    discussions=format_discussions(raw_heat),
                    tags=extract_tags(word + " " + note),
                    is_hot=raw_heat > 500000,
                ))
    except Exception as e:
        logger.warning("Weibo collection failed: %s", e)
    return results


async def collect_zhihu() -> list[dict]:
    """Zhihu hot-list: filter tax-related entries."""
    url = "https://www.zhihu.com/api/v3/feed/topstory/hot-lists/total"
    results = []
    try:
        async with make_client() as client:
            resp = await client.get(url, params={"limit": 50})
            if resp.status_code != 200:
                logger.warning("Zhihu returned HTTP %s", resp.status_code)
                return results
            data = resp.json()
            for item in data.get("data", []):
                target = item.get("target", {})
                title = target.get("title", "")
                excerpt = target.get("excerpt", "")
                if not is_tax_related(title + " " + excerpt):
                    continue
                detail_text = item.get("detail_text", "0")
                heat_num = int(re.sub(r"[^\d]", "", detail_text) or "0")
                answer_count = target.get("answer_count", 0)
                results.append(topic_dict(
                    title=title,
                    summary=excerpt[:200] if excerpt else title,
                    source="知乎",
                    platform_class=PLATFORM_ZHIHU,
                    author=target.get("author", {}).get("name", ""),
                    url=f"https://www.zhihu.com/question/{target.get('id', '')}",
                    heat=heat_num,
                    discussions=format_discussions(answer_count),
                    tags=extract_tags(title + " " + excerpt),
                    is_hot=heat_num > 1000000,
                ))
    except Exception as e:
        logger.warning("Zhihu collection failed: %s", e)
    return results


async def collect_bilibili() -> list[dict]:
    """Bilibili ranking (all categories): filter tax-related entries."""
    url = "https://api.bilibili.com/x/web-interface/ranking/v2"
    results = []
    try:
        async with make_client() as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Bilibili returned HTTP %s", resp.status_code)
                return results
            data = resp.json()
            for item in data.get("data", {}).get("list", []):
                title = item.get("title", "")
                desc = item.get("desc", "")
                if not is_tax_related(title + " " + desc):
                    continue
                stat = item.get("stat", {})
                view = stat.get("view", 0)
                danmaku = stat.get("danmaku", 0)
                like = stat.get("like", 0)
                reply = stat.get("reply", 0)
                heat = int(view * 0.1 + like * 2 + reply * 5 + danmaku * 1)
                results.append(topic_dict(
                    title=title,
                    summary=desc[:200] if desc else title,
                    source="B站",
                    platform_class=PLATFORM_BILIBILI,
                    author=item.get("owner", {}).get("name", ""),
                    url=f"https://www.bilibili.com/video/{item.get('bvid', '')}",
                    heat=heat,
                    discussions=format_discussions(danmaku + reply),
                    tags=extract_tags(title + " " + desc),
                    is_hot=view > 500000,
                ))
    except Exception as e:
        logger.warning("Bilibili collection failed: %s", e)
    return results


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
async def collect_all() -> list[dict]:
    tasks = [collect_weibo(), collect_zhihu(), collect_bilibili()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_items: list[dict] = []
    labels = ["Weibo", "Zhihu", "Bilibili"]
    for label, result in zip(labels, results):
        if isinstance(result, Exception):
            logger.error("%s raised: %s", label, result)
            continue
        logger.info("%s returned %d items", label, len(result))
        all_items.extend(result)

    # Deduplicate by title
    seen: set[str] = set()
    unique: list[dict] = []
    for item in all_items:
        if item["title"] not in seen:
            seen.add(item["title"])
            unique.append(item)

    # Sort by heat descending
    unique.sort(key=lambda x: x["heat"], reverse=True)

    # Mark top 3 as hot
    for i, item in enumerate(unique):
        item["isHot"] = i < 3

    return unique


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------
def save_json(topics: list[dict]) -> None:
    base = Path(os.getenv("DATA_DIR", "data"))
    now = NOW

    payload = {
        "updated_at": now.isoformat(),
        "count": len(topics),
        "topics": topics,
    }

    date_str = now.strftime("%Y-%m-%d")
    iso_cal = now.isocalendar()
    week_str = f"{iso_cal.year}-W{iso_cal.week:02d}"
    month_str = now.strftime("%Y-%m")

    daily_dir = base / "daily"
    weekly_dir = base / "weekly"
    monthly_dir = base / "monthly"

    for d in (daily_dir, weekly_dir, monthly_dir):
        d.mkdir(parents=True, exist_ok=True)

    daily_path = daily_dir / f"{date_str}.json"
    weekly_path = weekly_dir / f"{week_str}.json"
    monthly_path = monthly_dir / f"{month_str}.json"

    for path in (daily_path, weekly_path, monthly_path):
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Wrote %s  (%d topics)", path, len(topics))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    logger.info("=== tax-radar collector started at %s ===", NOW.strftime("%Y-%m-%d %H:%M %Z"))
    topics = await collect_all()
    if topics:
        save_json(topics)
        logger.info("Collection complete: %d topics saved.", len(topics))
    else:
        logger.warning("No tax-related topics found. JSON files NOT updated.")


if __name__ == "__main__":
    asyncio.run(main())
