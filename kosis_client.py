"""
KOSIS OpenAPI 클라이언트
KOSIS(국가통계포털)의 OpenAPI를 호출하는 비동기 클라이언트 모듈.

제공 기능:
  - 통계 카테고리 트리 탐색 (statisticsList)
  - 통계 데이터 조회 (statisticsData / statisticsParameterData)
  - 통계 설명 조회 (statisticsExplData)
  - KOSIS 통합 검색 (통계표명 키워드 검색)
"""

import httpx
from typing import Optional


BASE_URL = "https://kosis.kr/openapi"


class KosisClient:
    """KOSIS OpenAPI 비동기 HTTP 클라이언트."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    # ──────────────────────────────────────────────
    # 1. 통계 목록 (카테고리 트리)
    # ──────────────────────────────────────────────
    async def browse_categories(
        self,
        vw_cd: str = "MT_ZTITLE",
        parent_list_id: str = "A",
    ) -> list[dict]:
        """
        통계 카테고리 트리를 순회합니다.

        Args:
            vw_cd: 서비스뷰 코드
                MT_ZTITLE  - 국내통계 주제별 (기본값, 인구·가구 / 경제 / 사회 등)
                MT_OTITLE  - 국내통계 기관별
                MT_RTITLE  - 국제통계
                MT_BUKHAN  - 북한통계
                MT_TM1_TITLE - 대상별통계
                MT_TM2_TITLE - 이슈별통계
            parent_list_id: 시작 목록 ID ('A' = 최상위)

        Returns:
            LIST_ID/LIST_NM(중간 분류) 또는 ORG_ID/TBL_ID/TBL_NM(통계표) 포함 목록
        """
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "vwCd": vw_cd,
            "parentListId": parent_list_id,
            "format": "json",
            "jsonVD": "Y",
        }
        resp = await self._client.get(f"{BASE_URL}/statisticsList.do", params=params)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "err" in data:
            raise ValueError(f"KOSIS API 오류: {data}")
        return data if isinstance(data, list) else []

    # ──────────────────────────────────────────────
    # 2. 통계 데이터 — 파라미터 직접 지정 방식
    # ──────────────────────────────────────────────
    async def get_statistics_data(
        self,
        org_id: str,
        tbl_id: str,
        obj_l1: str = "ALL",
        itm_id: str = "ALL",
        prd_se: str = "Y",
        start_prd_de: Optional[str] = None,
        end_prd_de: Optional[str] = None,
        new_est_prd_cnt: Optional[int] = 10,
        prd_interval: Optional[int] = None,
        output_fields: Optional[str] = None,
    ) -> list[dict]:
        """
        통계표의 실제 수치 데이터를 조회합니다.

        Args:
            org_id: 기관코드 (예: '101' = 통계청)
            tbl_id: 통계표 ID (예: 'DT_1IN1502')
            obj_l1: 분류1 코드. 'ALL'이면 전체 분류값 반환
            itm_id: 항목 ID. 'ALL'이면 전체 항목 반환
            prd_se: 수록주기 (Y:연, M:월, Q:분기, H:반기, D:일, IR:부정기)
            start_prd_de: 시작시점 (예: '2015', '201501')
            end_prd_de: 종료시점 (예: '2023')
            new_est_prd_cnt: 최근 N개 시점 (start/end 미지정 시 사용)
            prd_interval: 수록시점 간격
            output_fields: 응답 필드 선택 (쉼표 구분)

        Returns:
            통계 행 목록. 각 행에 PRD_DE(시점), DT(수치), ITM_NM(항목명),
            C1_NM~C8_NM(분류명), UNIT_NM(단위) 등이 포함됩니다.
        """
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
        }
        if start_prd_de:
            params["startPrdDe"] = start_prd_de
        if end_prd_de:
            params["endPrdDe"] = end_prd_de
        if new_est_prd_cnt and not start_prd_de:
            params["newEstPrdCnt"] = str(new_est_prd_cnt)
        if prd_interval:
            params["prdInterval"] = str(prd_interval)
        if output_fields:
            params["outputFields"] = output_fields

        resp = await self._client.get(
            f"{BASE_URL}/Param/statisticsParameterData.do", params=params
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "err" in data:
            raise ValueError(f"KOSIS API 오류: {data}")
        return data if isinstance(data, list) else []

    # ──────────────────────────────────────────────
    # 3. 통계 설명 조회
    # ──────────────────────────────────────────────
    async def get_statistics_explanation(
        self,
        org_id: str,
        tbl_id: str,
        meta_itm: str = "ALL",
    ) -> list[dict]:
        """
        통계표의 조사 설명(목적, 주기, 대상 범위 등)을 가져옵니다.

        Args:
            org_id: 기관코드
            tbl_id: 통계표 ID
            meta_itm: 요청 항목 ('ALL' 또는 특정 필드명)

        Returns:
            통계 설명 정보 딕셔너리
        """
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "orgId": org_id,
            "tblId": tbl_id,
            "metaItm": meta_itm,
            "format": "json",
            "jsonVD": "Y",
        }
        resp = await self._client.get(
            f"{BASE_URL}/statisticsExplData.do", params=params
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else [data]

    # ──────────────────────────────────────────────
    # 4. KOSIS 통합검색 (통계표명 키워드 탐색)
    # ──────────────────────────────────────────────
    async def search_statistics(
        self,
        keyword: str,
        vw_cd: str = "MT_ZTITLE",
    ) -> list[dict]:
        """
        키워드로 통계표를 검색합니다.
        statisticsList + 필터링 방식 — KOSIS 통합검색 API가 열려 있으면
        statisticsSearch.do 엔드포인트를 시도하고, 실패 시 목록 탐색으로 대체합니다.

        Args:
            keyword: 검색 키워드 (예: '인구', '출생', '고령화')
            vw_cd: 서비스뷰 코드

        Returns:
            TBL_NM에 keyword가 포함된 통계표 목록 (ORG_ID, TBL_ID, TBL_NM 포함)
        """
        # 1) 통합검색 API 시도
        try:
            params = {
                "method": "getList",
                "apiKey": self.api_key,
                "vwCd": vw_cd,
                "searchNm": keyword,
                "format": "json",
                "jsonVD": "Y",
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

        # 2) 대체: 상위 카테고리 전체를 순회하며 필터링
        results = []
        top_level = await self.browse_categories(vw_cd=vw_cd, parent_list_id="A")
        for cat in top_level[:20]:  # 상위 20개 카테고리만 탐색
            list_id = cat.get("LIST_ID", "")
            if not list_id:
                continue
            try:
                children = await self.browse_categories(
                    vw_cd=vw_cd, parent_list_id=list_id
                )
                for item in children:
                    tbl_nm = item.get("TBL_NM", "")
                    if keyword in tbl_nm and item.get("TBL_ID"):
                        results.append(item)
            except Exception:
                continue
        return results
