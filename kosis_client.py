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
    # 대상별 (MT_TM1_TITLE)
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
    # 이슈별 (MT_TM2_TITLE)
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
    # 주제별 (MT_ZTITLE)
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
    # 지역통계 (MT_ATITLE01) — 시도·시군구 단위 지역 데이터 전용 뷰
    "지역": {
        "vw_cd": "MT_ATITLE01",
        "keywords": ["지역", "시도", "지역격차", "광역", "기초"],
        "topic_keywords": ["지역", "시도", "시군구", "지방", "광역시", "균형발전", "지방소멸"],
    },
    "지방지표": {
        "vw_cd": "MT_GTITLE01",
        "keywords": ["지방지표", "e-지방지표", "지역지표"],
        "topic_keywords": ["지방", "지역지표", "시군구지표", "생활지표"],
    },
    # 기관별 (MT_OTITLE) — 기관명 포함 쿼리
    "기관별": {
        "vw_cd": "MT_OTITLE",
        "keywords": ["통계청", "국가데이터처", "국토교통부", "보건복지부", "교육부",
                     "고용노동부", "행정안전부", "농림축산식품부", "산업통상자원부"],
        "topic_keywords": ["기관", "부처", "청", "원", "공단", "공사"],
    },
}

# 출력 텍스트 정규화 — Claude에게 반환하는 데이터에서만 구명칭을 신명칭으로 치환
# 검색 API 호출에는 적용하지 않음 (KOSIS는 여전히 구명칭 색인)
_OUTPUT_ALIAS: dict[str, str] = {
    "통계청": "국가데이터처",
}


def _normalize_output(text: str) -> str:
    """출력 텍스트의 기관명 등을 최신 명칭으로 치환."""
    for old, new in _OUTPUT_ALIAS.items():
        text = text.replace(old, new)
    return text


def detect_intent(query: str) -> list[dict]:
    matched = []
    query_lower = query.lower()
    for intent_key, config in INTENT_MAP.items():
        all_kws = [intent_key] + config.get("keywords", []) + config.get("topic_keywords", [])
        if any(kw in query_lower for kw in all_kws):
            matched.append({
                "intent": intent_key,
                "vw_cd": config["vw_cd"],
                "search_keywords": config["keywords"][:3],  # 검색어는 원본 유지
            })
    if not matched:
        words = [w for w in query.split() if len(w) >= 2][:3]
        matched.append({
            "intent": "일반",
            "vw_cd": "MT_ZTITLE",
            "search_keywords": words or [query[:10]],
        })
    return matched


# Module-level shared httpx client — one connection pool for all API keys
_shared_http_client = httpx.AsyncClient(timeout=30.0)


class KosisClient:
    """KOSIS OpenAPI async HTTP client (uses shared connection pool)."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = _shared_http_client

    async def close(self):
        pass  # shared client — do not close per instance

    # ── 1. Category browsing ─────────────────────────────────────────────────
    async def browse_categories(
        self,
        vw_cd: str = "MT_ZTITLE",
        parent_list_id: str = "A",
        _retries: int = 3,
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
        last_exc: Exception = RuntimeError("browse_categories: no attempts made")
        for attempt in range(_retries):
            try:
                resp = await self._client.get(
                    f"{BASE_URL}/statisticsList.do",
                    params=params,
                    timeout=45.0,
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict) and "err" in data:
                    # err 30 = 조회결과 없음 → 빈 배열로 처리 (에러 아님)
                    if data.get("err") == "30":
                        return []
                    raise ValueError(f"KOSIS API error: {data}")
                return data if isinstance(data, list) else []
            except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                last_exc = e
                if attempt < _retries - 1:
                    await asyncio.sleep(1.5 * (attempt + 1))
            except Exception as e:
                raise
        raise last_exc

    # ── 2. Probe table structure to discover valid parameters ────────────────
    async def _probe_table_params(
        self, org_id: str, tbl_id: str, prd_se: str = "Y"
    ) -> dict:
        """
        표 구조를 자동 탐지해 유효한 itmId 코드 목록과 차원(objL) 수를 반환.
        1개 기간 데이터를 최소 요청으로 시도, 응답 행에서 ITM_ID·C1~C8 추출.

        전략: objL 차원 수를 1→3으로 늘려가며 성공할 때까지 시도.
        두 엔드포인트 모두 실패하면 기본값 반환.

        반환값: {"itm_ids": [...], "n_dims": N}
        실패 시: {"itm_ids": [], "n_dims": 2}
        """
        for n in range(1, 4):  # objL 차원 1·2·3 순서로 시도
            probe = {
                "method": "getList",
                "apiKey": self.api_key,
                "orgId": org_id,
                "tblId": tbl_id,
                "prdSe": prd_se,
                "newEstPrdCnt": "1",
                "itmId": "ALL",
                "format": "json",
                "jsonVD": "Y",
                "errMsg": "Y",
            }
            for i in range(1, n + 1):
                probe[f"objL{i}"] = "ALL"

            for ep in [
                f"{BASE_URL}/Param/statisticsParameterData.do",
                f"{BASE_URL}/statisticsData.do",
            ]:
                try:
                    resp = await self._client.get(ep, params=probe, timeout=8.0)
                    rows = resp.json()
                    if not (isinstance(rows, list) and rows):
                        continue
                    # ITM_ID 중복 제거 (순서 유지)
                    seen: dict[str, bool] = {}
                    for r in rows:
                        itm = r.get("ITM_ID", "")
                        if itm and itm not in seen:
                            seen[itm] = True
                    # 실제 차원 수: C1~C8 중 값이 있는 최대 인덱스
                    row0 = rows[0]
                    actual = max(
                        (i for i in range(1, 9) if row0.get(f"C{i}")),
                        default=n,
                    )
                    return {"itm_ids": list(seen.keys()), "n_dims": actual}
                except Exception:
                    continue

        # 모든 시도 실패 → 2차원 기본값 (DT_FR0001N 등 2-dim 테이블 대응)
        return {"itm_ids": [], "n_dims": 2}

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

        def _build_params(**extra) -> dict:
            """공통 파라미터 베이스 생성."""
            p = {
                "method": "getList",
                "apiKey": self.api_key,
                "orgId": org_id,
                "tblId": tbl_id,
                "prdSe": prd_se,
                "format": "json",
                "jsonVD": "Y",
                "errMsg": "Y",
            }
            if start_prd_de:
                p["startPrdDe"] = start_prd_de
            if end_prd_de:
                p["endPrdDe"] = end_prd_de
            if new_est_prd_cnt and not start_prd_de:
                p["newEstPrdCnt"] = str(new_est_prd_cnt)
            p.update(extra)
            return p

        # ── 1차 시도: 주어진 파라미터로 Param 엔드포인트 ─────────────────────
        resp = await self._client.get(
            f"{BASE_URL}/Param/statisticsParameterData.do",
            params=_build_params(objL1=obj_l1, itmId=itm_id),
        )
        resp.raise_for_status()
        data = resp.json()

        # ── err 20: 차원/항목코드 불일치 → 표 구조 자동 탐지 후 재시도 ──────
        if isinstance(data, dict) and data.get("err") == "20":
            resolved = await self._probe_table_params(org_id, tbl_id, prd_se)
            n_dims = resolved["n_dims"]        # 실제 objL 차원 수
            itm_ids = resolved["itm_ids"]      # 유효 항목 코드 목록

            # 재시도 파라미터: 모든 차원 ALL + 구체적 itmId (없으면 ALL)
            retry_extra = {f"objL{i}": "ALL" for i in range(1, n_dims + 1)}
            retry_extra["itmId"] = "+".join(itm_ids[:30]) if itm_ids else "ALL"
            retry = _build_params(**retry_extra)

            # 2차: Param 엔드포인트
            resp2 = await self._client.get(
                f"{BASE_URL}/Param/statisticsParameterData.do", params=retry
            )
            resp2.raise_for_status()
            data = resp2.json()

            # 3차: 표준 statisticsData.do 폴백 (Param이 여전히 실패하는 경우)
            if isinstance(data, dict) and data.get("err") == "20":
                resp3 = await self._client.get(
                    f"{BASE_URL}/statisticsData.do", params=retry
                )
                resp3.raise_for_status()
                data = resp3.json()

        if isinstance(data, dict) and "err" in data:
            raise ValueError(
                f"KOSIS API error: {data}  request_id: {data.get('request_id', '')}"
            )
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
                "sort": "RANK",       # 정확도순 정렬 (가이드 명시값)
                "startCount": "1",
                "resultCount": "30",  # 페이지당 30개 (기본 20개보다 많이)
                "format": "json",
                "jsonVD": "Y",
                "errMsg": "Y",
            }
            resp = await self._client.get(
                f"{BASE_URL}/statisticsSearch.do", params=params, timeout=30.0
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception:
            pass

        # MT_OTITLE(기관별)·MT_GTITLE01(e-지방지표)은 parent_list_id="A" 미지원
        # → statisticsSearch.do 1차 검색으로 충분하므로 browse fallback 스킵
        _NO_BROWSE_FALLBACK = {"MT_OTITLE", "MT_GTITLE01"}
        if vw_cd in _NO_BROWSE_FALLBACK:
            return []

        try:
            top_level = await self.browse_categories(vw_cd=vw_cd, parent_list_id="A")
        except Exception:
            return []
        # 동시 요청 5개로 제한 — KOSIS 서버 연결 끊김 방지
        sem = asyncio.Semaphore(5)
        tasks = []
        for cat in top_level[:15]:
            list_id = cat.get("LIST_ID", "")
            if list_id:
                tasks.append(self._search_in_category(keyword, vw_cd, list_id, sem))
        results_nested = await asyncio.gather(*tasks, return_exceptions=True)
        results = []
        for r in results_nested:
            if isinstance(r, list):
                results.extend(r)
        return results

    async def _search_in_category(
        self, keyword: str, vw_cd: str, list_id: str,
        sem: asyncio.Semaphore | None = None,
    ) -> list[dict]:
        try:
            if sem:
                async with sem:
                    children = await self.browse_categories(vw_cd=vw_cd, parent_list_id=list_id)
            else:
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
                                "name": _normalize_output(r.get("TBL_NM", "")),
                                "updated": r.get("SEND_DE", ""),
                            })
                except Exception:
                    pass
            return found[:max_results]

        all_results_nested = await asyncio.gather(
            *[search_one(ic) for ic in intents], return_exceptions=True
        )
        merged: list[dict] = []
        seen_ids: set[str] = set()
        for batch in all_results_nested:
            if isinstance(batch, list):
                for item in batch:
                    uid = f"{item.get('org_id')}_{item.get('tbl_id')}"
                    if uid not in seen_ids:
                        seen_ids.add(uid)
                        merged.append(item)
        merged = merged[:max_results]
        return {
            "query": query,
            "intents": [i["intent"] for i in intents],
            "count": len(merged),
            "tables": merged,
        }
