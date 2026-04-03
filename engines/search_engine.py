"""
多引擎搜索模块
核心原则：Tavily搜不到就换Serper，再不行换Jina，最多试3次
"""

import asyncio
import os
import aiohttp
from typing import Optional
from config.settings import SEARCH_ENGINES, MAX_SEARCH_RETRIES
from utils.logger import setup_logger

logger = setup_logger("search_engine")


class SearchResult:
    def __init__(self, engine: str, query: str, content: str, success: bool):
        self.engine = engine
        self.query = query
        self.content = content
        self.success = success

    def __repr__(self):
        return f"SearchResult(engine={self.engine}, success={self.success}, len={len(self.content)})"


class MultiEngineSearcher:
    """
    多引擎搜索器：按优先级尝试，失败自动切换，最多3次
    """

    def __init__(self):
        self.engines = sorted(SEARCH_ENGINES, key=lambda x: x["priority"])

    async def search(self, query: str, context: str = "") -> Optional[SearchResult]:
        """
        对query进行搜索，依次尝试各引擎，最多3次
        返回第一个成功的结果，全部失败返回None（调用方决定排除）
        """
        full_query = f"{query} {context}".strip()
        attempts = 0

        for engine in self.engines:
            if attempts >= MAX_SEARCH_RETRIES:
                break

            attempts += 1
            engine_name = engine["name"]

            logger.info(f"[{engine['display']}] 搜索: {full_query} (第{attempts}次尝试)")

            result = await self._try_engine(engine, full_query)

            if result and result.success:
                logger.info(f"[{engine['display']}] ✓ 搜索成功")
                return result
            else:
                logger.warning(
                    f"[{engine['display']}] ✗ 搜索失败"
                    + (f"，切换到{self.engines[attempts]['display']}" if attempts < len(self.engines) else "，已无备用引擎")
                )

        logger.error(f"[搜索引擎] {MAX_SEARCH_RETRIES}次全部失败: {query} → 此数据点标记为缺失")
        return None

    async def _try_engine(self, engine: dict, query: str) -> Optional[SearchResult]:
        """调用具体引擎"""
        name = engine["name"]
        api_key = os.environ.get(engine["api_key_env"], "")

        try:
            if name == "tavily":
                return await self._tavily(query, api_key, engine["timeout"])
            elif name == "serper":
                return await self._serper(query, api_key, engine["timeout"])
            elif name == "firecrawl":
                return await self._firecrawl(query, api_key, engine["timeout"])
            elif name == "jina":
                return await self._jina(query, api_key, engine["timeout"])
            elif name == "feishu":
                return await self._feishu(query, api_key, engine["timeout"])
        except asyncio.TimeoutError:
            logger.warning(f"[{name}] 超时 ({engine['timeout']}s)")
        except Exception as e:
            logger.warning(f"[{name}] 异常: {e}")
        return None

    async def _tavily(self, query: str, api_key: str, timeout: int) -> Optional[SearchResult]:
        if not api_key:
            logger.debug("[Tavily] 未配置API Key，跳过")
            return None

        payload = {
            "api_key": api_key,
            "query": query,
            "search_depth": "advanced",
            "include_answer": True,
            "max_results": 5,
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.post("https://api.tavily.com/search", json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data.get("answer", "") + " " + " ".join(
                        r.get("content", "") for r in data.get("results", [])[:3]
                    )
                    return SearchResult("tavily", query, content, bool(content.strip()))
        return None

    async def _serper(self, query: str, api_key: str, timeout: int) -> Optional[SearchResult]:
        if not api_key:
            logger.debug("[Serper] 未配置API Key，跳过")
            return None

        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        payload = {"q": query, "num": 5, "gl": "cn", "hl": "zh-cn"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.post("https://google.serper.dev/search", json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    snippets = " ".join(
                        item.get("snippet", "") for item in data.get("organic", [])[:5]
                    )
                    return SearchResult("serper", query, snippets, bool(snippets.strip()))
        return None

    async def _firecrawl(self, query: str, api_key: str, timeout: int) -> Optional[SearchResult]:
        if not api_key:
            logger.debug("[Firecrawl] 未配置API Key，跳过")
            return None

        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"query": query, "limit": 3}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.post("https://api.firecrawl.dev/v1/search", json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = " ".join(
                        r.get("markdown", r.get("content", ""))[:500]
                        for r in data.get("data", [])[:3]
                    )
                    return SearchResult("firecrawl", query, content, bool(content.strip()))
        return None

    async def _jina(self, query: str, api_key: str, timeout: int) -> Optional[SearchResult]:
        """Jina AI Reader - 直接读取搜索结果页"""
        if not api_key:
            logger.debug("[Jina] 未配置API Key，跳过")
            return None

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }
        encoded = query.replace(" ", "+")
        url = f"https://s.jina.ai/{encoded}"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    return SearchResult("jina", query, content[:2000], bool(content.strip()))
        return None

    async def _feishu(self, query: str, api_key: str, timeout: int) -> Optional[SearchResult]:
        """飞书内部知识库搜索（适合内部研报）"""
        if not api_key:
            logger.debug("[飞书] 未配置API Key，跳过")
            return None
        # 飞书搜索API实现（企业自定义）
        logger.debug("[飞书] 飞书搜索需企业自定义集成")
        return None
