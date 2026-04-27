"""
KOSIS OpenAPI client
- Intent-based search: maps policy/research needs to KOSIS categories
- Parallel multi-keyword search
- Auto-retry on objL error by fetching actual item codes
"""

import asyncio
import httpx
from typing import Optional


BASE_URL = "https://kosis.kr/openapi"

# ─────────────────────────────────────────────────────────────────────────────
# intent → KOSIS search keyword mapping
# ─────────────────────────────────────────────────────────────────────────────
INTENT_MAP: dict[str, dict] = {
    # 대상별
    "청년": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["청년", "청년층", "청년고용", "청년취업"],
        "topic_keywords": ["청년", "고용", "취업", "실업", "주거", "교육"],
    },
    "아동": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["아동", "어린이", "보육", "아동복지"],
        "topic_keywords": ["아동", "보육", "유아", "어린이집", "아동학대"],
    },
    "청소년": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["청소년", "청소년범죄", "학교폭력"],
        "topic_keywords": ["청소년", "학교", "학업", "비행"],
    },
    "노인": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["노인", "고령자", "고령인구", "노인복지"],
        "topic_keywords": ["노인", "고령", "65세", "노년", "치매", "요양"],
    },
    "여성": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["여성", "여성고용", "여성경제"],
        "topic_keywords": ["여성", "모성", "성별", "경력단절", "여성취업"],
    },
    "장애인": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["장애인", "장애", "장애등급"],
        "topic_keywords": ["장애인", "장애", "복지", "재활"],
    },
    "다문화": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["다문화", "외국인", "결혼이민"],
        "topic_keywords": ["다문화", "외국인", "이민", "귀화"],
    },
    "한부모": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["한부모", "모자가정", "부자가정"],
        "topic_keywords": ["한부모", "편부", "편모", "모자", "부자가정", "저소득"],
    },
    # 이슈별
    "저출산": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["저출산", "출산", "출생"],
        "topic_keywords": ["출산", "출생", "합계출산율", "신생아", "저출생"],
    },
    "고령화": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["고령화", "고령사회", "초고령"],
        "topic_keywords": ["고령화", "고령인구", "노인인구", "고령화율"],
    },
    "인구소멸": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["인구소멸", "인구감소", "인구절벽"],
        "topic_keywords": ["인구", "출생", "사망", "합계출산율", "인구감소"],
    },
    "1인가구": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["1인가구", "단독가구", "혼자"],
        "topic_keywords": ["1인가구", "단독가구", "혼인", "비혼"],
    },
    "저소득": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["저소득", "기초생활", "빈곤"],
        "topic_keywords": ["저소득", "기초생활", "수급자", "빈곤율", "차상위"],
    },
    # 주제별
    "고용": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["고용", "취업", "실업"],
        "topic_keywords": ["고용률", "실업률", "취업자", "경제활동인구"],
    },
    "교육": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["교육", "학교", "학생"],
        "topic_keywords": ["교육", "학생수", "학교수", "대학", "진학"],
    },
    "주거": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["주택", "주거", "전세"],
        "topic_keywords": ["주택", "주거", "전세", "월세", "아파트", "주택보급"],
    },
    "소득": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["소득", "임금", "가계"],
        "topic_keywords": ["소득", "임금", "급여", "가계소득", "소득분배"],
    },
    "복지": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["복지", "사회보장", "급여"],
        "topic_keywords": ["사회복지", "복지급여", "사회보장", "복지지출"],
    },
    "보건": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["보건", "의료", "건강"],
        "topic_keywords": ["보건", "의료", "건강", "병원", "사망원인"],
    },
    "인구": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["인구", "인구수", "인구통계"],
        "topic_keywords": ["인구", "출생", "사망", "이동", "인구구조"],
    },
    "지역": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["지역", "시도", "지역격차"],
        "topic_keywords": ["지역", "시도", "지방", "광역시", "균형발전"],
    },
    "프랜차이즈": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["프랜차이즈", "가맹점", "가맹사업"],
        "topic_keywords": ["프랜차이즈", "가맹", "편의점", "외식"],
    },
    "소상공인": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["소상공인", "자영업", "중소기업"],
        "topic_keywords": ["소상공인", "자영업", "소기업", "창업"],
    },
}


def detect_intent(query: str) -> list[dict]:
    matched = []
    query_lower = query.lower()
    for intent_key, config in INTENT_MAP.items():
        all_kws = [intent_key] + config.get("keywords", []) + config.get("topic_keywords", [])
        if any(kw in query_lower for kw in all_kws):
            matched.append({
                "intent": intent_key,
                "vw_cd": config["vw_cd"],
                "search_keywords": config["keywords"][:3],
            })
    if not matched:
        words = [w for w in query.split() if len(w) >= 2][:3]
        matched.append({
            "intent": "일반",
            "vw_cd": "MT_ZTITLE",
            "search_keywords": words or [query[:10]],
        })
    return matched


class KosisClient:
    """KOSIS OpenAPI async HTTP client."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    # ── 1. Category browsing ─────────────────────────────────────────────────
    async def browse_categories(
        self,
        vw_cd: str = "MT_ZTITLE",
        parent_list_id: str = "A",
    ) -> list[dict]:
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "vwCd": vw_cd,
            "parentListId": parent_list_id,
            "format": "json",
            "jsonVD": "Y",
            "errMsg": "Y",
        }
        resp = await self._client.get(f"{BASE_URL}/statisticsList.do", params=params)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "err" in data:
            raise ValueError(f"KOSIS API error: {data}")
        return data if isinstance(data, list) else []

    # ── 2. Fetch item/dimension codes for a table ────────────────────────────
    async def _get_item_codes(self, org_id: str, tbl_id: str) -> list[dict]:
        """
        Returns the list of classification dimensions for a table.
        Used to build objL1/objL2/... parameters when 'ALL' is rejected.
        """
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "orgId": org_id,
            "tblId": tbl_id,
            "itmDiv": "all",
            "format": "json",
            "jsonVD": "Y",
            "errMsg": "Y",
        }
        try:
            resp = await self._client.get(f"{BASE_URL}/statisticsItemList.do", params=params)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []

    # ── 3. Statistics data ───────────────────────────────────────────────────
    async def get_statistics_data(
        self,
        org_id: str,
        tbl_id: str,
        obj_l1: str = "ALL",
        itm_id: str = "ALL",
        prd_se: str = "Y",
        start_prd_de: Optional[str] = None,
        end_prd_de: Optional[str] = None,
        new_est_prd_cnt: Optional[int] = 15,
    ) -> list[dict]:
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "orgId": org_id,
            "tblId": tbl_id,
            "objL1": obj_l1,
            "itmId": itm_id,
            "prdSe": prd_se,
            "format": "json",
            "jsonVD": "Y",
            "errMsg": "Y",
        }
        if start_prd_de:
            params["startPrdDe"] = start_prd_de
        if end_prd_de:
            params["endPrdDe"] = end_prd_de
        if new_est_prd_cnt and not start_prd_de:
            params["newEstPrdCnt"] = str(new_est_prd_cnt)

        resp = await self._client.get(
            f"{BASE_URL}/Param/statisticsParameterData.do", params=params
        )
        resp.raise_for_status()
        data = resp.json()

        # ── objL error: fetch actual dimension codes and retry ───────────────
        if isinstance(data, dict) and data.get("err") == "20":
            items = await self._get_item_codes(org_id, tbl_id)
            if items:
                # Collect unique OBJ_ID values from items
                obj_ids = []
                seen = set()
                for it in items:
                    oid = it.get("OBJ_ID") or it.get("ITMC_ID") or ""
                    if oid and oid not in seen:
                        seen.add(oid)
                        obj_ids.append(oid)
                # Rebuild params with actual objL values
                params.pop("objL1", None)
                params.pop("objL2", None)
                params.pop("objL3", None)
                for idx, oid in enumerate(obj_ids[:5], start=1):
                    params[f"objL{idx}"] = oid
                resp2 = await self._client.get(
                    f"{BASE_URL}/Param/statisticsParameterData.do", params=params
                )
                resp2.raise_for_status()
                data = resp2.json()

        if isinstance(data, dict) and "err" in data:
            raise ValueError(f"KOSIS API error: {data}  request_id: {data.get('request_id', '')}")
        return data if isinstance(data, list) else []

    # ── 4. Statistics explanation ────────────────────────────────────────────
    async def get_statistics_explanation(
        self,
        org_id: str,
        tbl_id: str,
        meta_itm: str = "ALL",
    ) -> list[dict]:
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "orgId": org_id,
            "tblId": tbl_id,
            "metaItm": meta_itm,
            "format": "json",
            "jsonVD": "Y",
            "errMsg": "Y",
        }
        resp = await self._client.get(
            f"{BASE_URL}/statisticsExplData.do", params=params
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else [data]

    # ── 5. Keyword search ────────────────────────────────────────────────────
    async def search_statistics(
        self,
        keyword: str,
        vw_cd: str = "MT_ZTITLE",
    ) -> list[dict]:
        try:
            params = {
                "method": "getList",
                "apiKey": self.api_key,
                "vwCd": vw_cd,
                "searchNm": keyword,
                "format": "json",
                "jsonVD": "Y",
                "errMsg": "Y",
            }
            resp = await self._client.get(
                f"{BASE_URL}/statisticsSearch.do", params=params
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception:
            pass

        top_level = await self.browse_categories(vw_cd=vw_cd, parent_list_id="A")
        tasks = []
        for cat in top_level[:15]:
            list_id = cat.get("LIST_ID", "")
            if list_id:
                tasks.append(self._search_in_category(keyword, vw_cd, list_id))
        results_nested = await asyncio.gather(*tasks, return_exceptions=True)
        results = []
        for r in results_nested:
            if isinstance(r, list):
                results.extend(r)
        return results

    async def _search_in_category(self, keyword: str, vw_cd: str, list_id: str) -> list[dict]:
        try:
            children = await self.browse_categories(vw_cd=vw_cd, parent_list_id=list_id)
            return [
                item for item in children
                if keyword in item.get("TBL_NM", "") and item.get("TBL_ID")
            ]
        except Exception:
            return []

    # ── 6. Intent-based unified search ──────────────────────────────────────
    async def search_by_intent(
        self,
        query: str,
        max_results: int = 15,
    ) -> dict:
        intents = detect_intent(query)

        async def search_one(intent_cfg: dict) -> list[dict]:
            found = []
            for kw in intent_cfg["search_keywords"]:
                try:
                    results = await self.search_statistics(
                        keyword=kw, vw_cd=intent_cfg["vw_cd"]
                    )
                    for r in results:
                        if r.get("TBL_ID") and r not in found:
                            found.append({
                                "org_id": r.get("ORG_ID", ""),
                                "tbl_id": r.get("TBL_ID", ""),
                                "name": r.get("TBL_NM", ""),
                                "category": intent_cfg["intent"],
                                "vw_cd": intent_cfg["vw_cd"],
                            })
                except Exception:
                    pass
            return found

        tasks = [search_one(cfg) for cfg in intents]
        nested = await asyncio.gather(*tasks, return_exceptions=True)

        all_tables = []
        seen_ids = set()
        for group in nested:
            if isinstance(group, list):
                for t in group:
                    key = (t["org_id"], t["tbl_id"])
                    if key not in seen_ids:
                        seen_ids.add(key)
                        all_tables.append(t)

        return {
            "detected_intents": [i["intent"] for i in intents],
            "query": query,
            "total_found": len(all_tables),
            "tables": all_tables[:max_results],
        }
