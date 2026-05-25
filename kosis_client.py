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
    # ── 대상별 (MT_TM1_TITLE) ────────────────────────────────────────────────
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
        "topic_keywords": ["노인", "고령자", "고령인구", "65세", "노년", "치매", "요양"],
        # "고령" 제거 — "고령화" 쿼리에서 노인 인텐트 오발동 유발 (고령화 전용 인텐트가 따로 있음)
    },
    "여성": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["여성", "여성고용", "여성경제"],
        "topic_keywords": ["여성", "모성", "경력단절", "여성취업"],  # "성별" 제거 — 차원어라 오매칭 유발
    },
    "장애인": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["장애인", "장애", "장애등급"],
        "topic_keywords": ["장애인", "장애등록", "장애급여", "재활"],
        # "복지" 제거 — "복지서비스" 등 일반 복지 쿼리에서 장애인 인텐트 오발동 유발
    },
    "다문화": {
        "vw_cd": "MT_TM1_TITLE",
        "keywords": ["다문화", "외국인", "결혼이민"],
        "topic_keywords": ["다문화", "외국인", "이민", "귀화"],
    },
    "한부모": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["한부모", "모자가정", "부자가정"],
        "topic_keywords": ["한부모", "편부", "편모", "모자", "부자가정"],
        # "저소득" 제거 — 저소득 인텐트와 충돌, 한부모 쿼리에서 저소득 의도를 강요하지 않음
    },
    # ── 이슈별 (MT_TM2_TITLE) ────────────────────────────────────────────────
    "저출산": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["저출산", "저출생", "출산율", "합계출산율", "출생아", "출생아수",
                     "출산", "신생아", "출산통계", "아기", "아이"],
        "topic_keywords": ["저출산", "출산", "합계출산율", "신생아", "저출생"],
        # "저출생" 키워드 추가 — 공식 대체 용어로 점점 많이 쓰임
        # "출산"(2글자) 토큰 추가 — "출산 감소", "출산 통계" 쿼리 대응
        # "신생아"(3글자) keywords로 승격 — "신생아 감소" 쿼리에서 topic_keywords만으로 미감지
        # "아기"(2글자) 토큰 추가 — "아기 안 낳는", "아기 출생" 등 구어체 대응
        # "출산통계"(4글자) 추가 — "출산 통계" substring 미감지 보완
    },
    "고령화": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["고령화", "고령사회", "초고령"],
        "topic_keywords": ["고령화", "고령인구", "노인인구", "고령화율"],
    },
    "인구소멸": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["인구소멸", "인구감소", "인구절벽"],
        "topic_keywords": ["인구소멸", "인구감소", "합계출산율"],
        # "인구"·"출생"·"사망" 제거 — 2글자 토큰이라 "제조업 사망", "산업 이동" 등에서
        # 인구소멸 인텐트가 오발동됨. 인구소멸은 키워드로만 명확히 매칭
    },
    "1인가구": {
        "vw_cd": "MT_TM2_TITLE",
        "keywords": ["1인가구", "1인 가구", "단독가구", "혼자", "혼자사는", "독거", "독거노인"],
        "topic_keywords": ["1인가구", "단독가구", "혼인", "비혼"],
        # "1인 가구"(띄어쓰기 포함, 5글자→substring) 추가 — 사용자들이 공백 포함 입력 多
        # "독거"(2글자 토큰) 추가 — "독거노인", "독거 가구" 구어체 대응
    },
    # ── 주제별 (MT_ZTITLE) ───────────────────────────────────────────────────
    "저소득": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["저소득", "기초생활", "빈곤"],
        "topic_keywords": ["저소득", "기초생활수급", "빈곤율", "차상위"],
        # "수급자" 제거 — "국민연금 수급자", "건강보험 수급자" 등에서 저소득 오매칭 유발
    },
    "고용": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["고용", "취업", "실업", "고용보험", "경제활동"],
        "topic_keywords": ["고용률", "실업률", "취업자", "경제활동인구", "구직"],
        # "고용보험" 추가 — "고용보험 가입자" 쿼리에서 고용 인텐트 미감지 문제 수정
    },
    "교육": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["교육", "학교", "학생", "교육비", "학력"],
        "topic_keywords": ["교육", "학생수", "학교수", "대학", "진학", "수업료"],
        # "교육비" 추가 — "교육비 지출" 쿼리에서 교육 인텐트 미감지 문제 수정
    },
    "주거": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["주택", "주거", "전세", "집값", "전셋값", "매매가",
                     "주택가격", "주택매매", "부동산", "아파트가격"],
        "topic_keywords": ["주택", "주거", "전세", "월세", "아파트", "주택보급"],
        # "집값"(3글자, substring) 추가 — "집값 오른 거" 등 구어체 핵심 키워드
        # "전셋값"(4글자)·"매매가"(3글자) 추가 — 구어체·약식 표현 대응
        # "아파트가격"(5글자) 추가 — "아파트가격 통계" 쿼리 대응
    },
    "소득": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["소득", "임금", "가계"],
        "topic_keywords": ["소득분배", "임금", "급여", "가계소득", "지니계수"],
        # "소득분배"를 topic_keywords로 이동 — 저소득과 구분 명확화
    },
    "복지": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["복지", "사회보장", "사회서비스"],
        "topic_keywords": ["사회복지", "복지급여", "사회보장", "복지지출"],
        # "국민연금"·"기초연금"·"연금보험" 제거 → 전용 "연금" 인텐트로 이동
        # "급여" 제거 — "급여" 단독으로 복지 오매칭 유발 가능 (소득 인텐트와 혼선)
    },
    "연금": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["국민연금", "기초연금", "연금수급", "수급권자", "연금급여",
                     "노령연금", "유족연금", "장애연금", "연금통계",
                     "수급액", "지급액", "연금액", "노인연금"],
        "topic_keywords": ["국민연금", "기초연금", "연금수급", "수급자", "연금급여"],
        # "수급액"·"지급액"·"연금액"(3글자 substring) — "기초연금 지급액", "연금액 얼마" 등 직접 표현
        # "노인연금"(4글자) — "노인들이 받는 연금" 구어체 단축 표현 대응
        # "용돈" 제외 — 2글자 exact이나 "학생 용돈" 등 false positive 위험
        # 국민연금공단 통계(연금수급유형·지역별 수급자 등)는 MT_ZTITLE에서 검색할 때
        # "연금수급"·"수급권자"·"노령연금" 등 구체적 키워드로 올바른 표가 상위 노출됨
    },
    "자살": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["자살", "자살률", "자살율", "자해", "고의적자해"],
        "topic_keywords": ["자살", "자살률", "자해", "자살예방", "정신건강"],
    },
    "보건": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["보건", "의료", "건강", "건강보험", "의료비"],
        "topic_keywords": ["보건", "의료", "건강", "병원", "사망원인", "질병"],
        # "건강보험"·"의료비" 추가 — "건강보험 현황" 쿼리에서 보건 인텐트 미감지 문제 수정
    },
    "인구": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["인구", "인구수", "인구통계", "인구구조", "인구이동"],
        "topic_keywords": ["인구구조", "인구분포", "인구변동"],
        # "출생"·"사망"·"이동" 제거 — 2글자 토큰이라 "제조업 이동", "산업재해 사망" 등에서 오매칭 유발
        # 인구 관련 복합어("인구구조"·"인구이동") 는 3글자↑라 substring 매칭 가능
    },
    "제조업": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["제조업", "광공업", "제조업체", "제조업통계"],
        "topic_keywords": ["제조업", "광공업", "생산지수", "출하액", "제조업체수"],
    },
    "산업": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["산업별", "사업체", "산업통계", "산업구조"],
        "topic_keywords": ["사업체수", "종사자수", "산업재해", "산업생산"],
    },
    "농림어업": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["농업", "농림", "어업", "농가", "축산", "농업통계", "농림어업"],
        "topic_keywords": ["농업생산", "어업생산", "농가수", "농가인구", "축산업"],
        # "농업통계"·"농림어업" 추가 — "농업"이 2글자라 복합어("농업통계") 토큰에서 미감지
    },
    "무역": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["수출", "수입", "무역", "수출입"],
        "topic_keywords": ["수출액", "수입액", "무역수지", "수출입통계"],
    },
    "물가": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["물가", "소비자물가", "생산자물가", "물가지수"],
        "topic_keywords": ["소비자물가지수", "생산자물가지수", "물가상승률", "인플레이션"],
    },
    "에너지": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["에너지", "전력", "석유", "가스"],
        "topic_keywords": ["에너지소비", "전력사용량", "에너지생산", "신재생에너지"],
    },
    "국민계정": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["gdp", "gni", "국민계정", "국내총생산", "경제성장률",
                     "경제성장", "성장률", "경기침체", "경기둔화", "경기불황", "불황", "경제 성장", "경기 침체", "성장 둔화", "성장 정체"],
        "topic_keywords": ["국내총생산", "경제성장률", "국민총소득", "소비지출"],
        # 영문 키워드는 소문자로 — _kw_matches가 query_lower(소문자 변환)와 비교하므로
        # "GDP" 대문자 키워드는 "gdp 성장률" 쿼리에서 미감지됨
        # "경제성장"(4글자, substring) — "경제성장률", "경제성장 둔화" 커버
        # "성장률"(3글자, substring) — "성장률 하락", "성장률 추이" 쿼리 대응
        # "경기침체"·"경기둔화"·"경기불황"(4글자, substring) — "경기 침체" 구어체는
        #   공백 때문에 미매칭이지만 "경기침체", "경기둔화" 붙여쓰기 쿼리 대응
    },
    "환경": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["환경", "기후", "온실가스", "폐기물"],
        "topic_keywords": ["환경오염", "탄소배출", "온실가스", "폐기물처리"],
    },
    "건설": {
        "vw_cd": "MT_ZTITLE",
        "keywords": ["건설", "건설업", "건축"],
        "topic_keywords": ["건설수주", "건설공사", "착공", "건축허가"],
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
    # ── 지역통계 (MT_ATITLE01) — 시도·시군구 단위 지역 데이터 전용 뷰 ───────
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
    # ── 기관별 (MT_OTITLE) — 기관명 포함 쿼리 ──────────────────────────────
    "기관별": {
        "vw_cd": "MT_OTITLE",
        "keywords": ["통계청", "국가데이터처", "국토교통부", "보건복지부", "교육부",
                     "고용노동부", "행정안전부", "농림축산식품부", "산업통상자원부"],
        "topic_keywords": ["기관별통계", "부처통계", "공단통계", "공사통계"],
        # "청", "원" 제거 — 1글자라 "청년", "원인" 등에 substring 오매칭
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


# 검색 시 제거할 한국어 불용어 (조사·동사·일반명사·차원어 등 KOSIS 검색에 무의미한 단어)
_STOPWORDS = {
    # 요청 동사·조사
    "통계", "찾아줘", "알려줘", "보여줘", "데이터", "현황", "분석", "조회",
    "정보", "자료", "관련", "있어", "있나", "있나요", "줘", "해줘",
    "알고싶어", "궁금해", "뭐야", "어때", "어떻게", "최근", "최신",
    "한국", "대한민국", "전국", "우리나라",
    # 차원·분류어 — 표 구조를 설명할 뿐, 검색 키워드로 쓰면 노이즈
    "연령별", "성별", "지역별", "시도별", "시군구별", "월별", "분기별", "연도별",
    "연령", "성", "지역", "시도", "시군구", "연도", "기간", "추이",
    "남성", "여성", "남녀", "남자", "여자",
    "사망원인통계", "사망원인",  # 카테고리명보다 구체 주제어로 검색하는 게 정확
}

# 한국어 조사 목록 (긴 것부터 — 최장 일치 우선)
_KO_PARTICLES: list[str] = sorted(
    {"이가","가","이","을","를","은","는","의","와","과","으로","로","에서","에게",
     "부터","까지","도","만","이나","나","이랑","랑","에","라","이라","들","께서",
     "한테","한테서","보다","처럼","만큼","이면","면","고","며","이며","아","야"},
    key=len, reverse=True,
)


def _strip_particle(token: str) -> str:
    """토큰 끝에 붙은 한국어 조사를 제거. 어근이 2글자 미만이 되면 제거 안 함."""
    if len(token) <= 2:
        return token
    for p in _KO_PARTICLES:
        if token.endswith(p) and len(token) - len(p) >= 2:
            return token[:-len(p)]
    return token


# 일반 인텐트 폴백 시 병렬 검색할 추가 vw_cd 목록
_FALLBACK_VW_CDS = ["MT_ZTITLE", "MT_TM1_TITLE", "MT_TM2_TITLE"]



_SEARCH_STOPWORDS = {
    "통계", "자료", "데이터", "현황", "관련", "분석", "조회", "결과",
    "있어", "줘", "알려", "보여", "찾아", "한국", "대한민국", "전국",
}

def _score_tbl(tbl_nm: str, query_tokens: set) -> float:
    """표명(TBL_NM)과 쿼리 토큰의 겹침 점수 (0.0 ~ 1.0).
    매칭 토큰이 많을수록, 표명이 짧을수록(정확도 높을수록) 높은 점수."""
    if not tbl_nm or not query_tokens:
        return 0.0
    nm = tbl_nm.lower()
    matched = [t for t in query_tokens if len(t) >= 2 and t in nm]
    if not matched:
        return 0.0
    # 겹친 토큰 수 / 전체 쿼리 토큰 수 (recall) * 겹친 토큰 수 / 표명 글자 수 (precision proxy)
    recall    = len(matched) / max(len(query_tokens), 1)
    precision = sum(len(t) for t in matched) / max(len(tbl_nm), 1)
    return round((recall + precision) / 2, 4)

def detect_intent(query: str) -> list[dict]:
    matched = []
    query_lower = query.lower()
    raw_tokens = set(query_lower.split())
    # 조사 제거 토큰도 추가 — "아이가"→"아이", "집값이"→"집값" 등 구어체 대응
    query_tokens = raw_tokens | {_strip_particle(t) for t in raw_tokens}

    def _kw_matches(kw: str) -> bool:
        """
        키워드 매칭 규칙:
        - 대소문자 무관 비교 (query_lower vs kw.lower())
        - 3글자 이상: substring 허용 (ex. "청년" in "청년실업률", "gdp" in "gdp 성장률")
        - 2글자 이하: 토큰 완전일치만 허용 (ex. "청"은 "청년" substring 매칭 차단)
        """
        kw_lower = kw.lower()
        if len(kw) <= 2:
            return kw_lower in query_tokens
        return kw_lower in query_lower

    for intent_key, config in INTENT_MAP.items():
        all_kws = [intent_key] + config.get("keywords", []) + config.get("topic_keywords", [])
        if any(_kw_matches(kw) for kw in all_kws):
            matched.append({
                "intent": intent_key,
                "vw_cd": config["vw_cd"],
                "search_keywords": config["keywords"][:3],
            })
    if not matched:
        # 불용어 제거 후 의미 있는 단어만 추출
        clean = [w for w in query.split() if len(w) >= 2 and w not in _STOPWORDS]
        keywords = clean[:3] or [query[:10]]
        # 다중 vw_cd에서 병렬 검색되도록 각각 등록
        for vw_cd in _FALLBACK_VW_CDS:
            matched.append({
                "intent": "일반",
                "vw_cd": vw_cd,
                "search_keywords": keywords,
            })
    return matched


# Module-level shared httpx client — one connection pool for all API keys
# keepalive_expiry=10: KOSIS 서버가 idle 연결을 먼저 닫기 전에 클라이언트가 먼저 정리
# max_connections=8: Railway Hobby 플랜(0.5GB) 메모리 절감 — 연결당 ~수십KB 소비
# max_keepalive_connections=5: keep-alive pool 상한 (stale connection hang 방지)
_shared_http_client = httpx.AsyncClient(
    timeout=15.0,
    limits=httpx.Limits(
        max_connections=8,
        max_keepalive_connections=5,
        keepalive_expiry=10.0,
    ),
)

# 전역 KOSIS API 동시 요청 제한 — 다수 인텐트 병렬 검색 시 rate limit·큐 대기 방지
_KOSIS_GLOBAL_SEM = asyncio.Semaphore(8)


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
                async with _KOSIS_GLOBAL_SEM:
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
        표 구조를 자동 탐지해 유효한 itmId 코드 목록, 차원(objL) 수,
        실제 수록주기(prd_se)를 반환.

        전략:
          1. 주어진 prd_se(기본 Y)로 objL 차원 1~3 시도
          2. 전부 실패하면 prd_se = M(월), Q(분기)로 재시도
          3. 두 엔드포인트 모두 실패하면 기본값 반환

        반환값: {"itm_ids": [...], "n_dims": N, "prd_se": "Y"|"M"|"Q"}
        실패 시: {"itm_ids": [], "n_dims": 2, "prd_se": prd_se}
        """
        # prd_se 후보: 주어진 값 우선, 그 다음 나머지 순서로 시도
        all_prd = ["Y", "M", "Q"]
        prd_candidates = [prd_se] + [p for p in all_prd if p != prd_se]

        for cur_prd in prd_candidates:
            for n in range(1, 4):  # objL 차원 1·2·3
                probe = {
                    "method": "getList",
                    "apiKey": self.api_key,
                    "orgId": org_id,
                    "tblId": tbl_id,
                    "prdSe": cur_prd,
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
                        seen: dict[str, bool] = {}
                        for r in rows:
                            itm = r.get("ITM_ID", "")
                            if itm and itm not in seen:
                                seen[itm] = True
                        row0 = rows[0]
                        actual = max(
                            (i for i in range(1, 9) if row0.get(f"C{i}")),
                            default=n,
                        )
                        # "계/합계/전국" 코드 추출 — ALL 대신 집계 코드만 요청해 셀 수 절감
                        total_codes: dict[str, str] = {}
                        _total_kws = {"계", "합계", "전국", "전체", "소계"}
                        for r in rows:
                            for ci in range(1, 9):
                                dim_nm = r.get(f"C{ci}_NM", "")
                                dim_val = r.get(f"C{ci}", "")
                                if dim_val and dim_nm in _total_kws:
                                    total_codes[str(ci)] = dim_val
                        return {
                            "itm_ids": list(seen.keys()),
                            "n_dims": actual,
                            "prd_se": cur_prd,
                            "total_codes": total_codes,  # {dim_idx: code} 집계 코드
                        }
                    except Exception:
                        continue

        return {"itm_ids": [], "n_dims": 2, "prd_se": prd_se}

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
        breakdown: bool = False,
        expand_c1: bool = False,
    ) -> list[dict]:
        # expand_c1=True: C1(첫 번째 차원)만 ALL, 나머지는 "계" 집계 코드
        # → filter_keyword 사용 시 원인명·항목명이 C1_NM에 나타나도록 하기 위해 사용

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

        # ═══════════════════════════════════════════════════════════════════
        # 핵심 설계:
        #   Phase 1 (Discovery) — newEstPrdCnt=1만 사용, 날짜 범위 없음
        #     objL1=ALL → err:20 → objL2=ALL → ... → objL4=ALL
        #     날짜 파라미터(startPrdDe/endPrdDe)를 함께 보내면 Param 엔드포인트가
        #     err:20을 반복하므로, discovery는 반드시 newEstPrdCnt로만 수행해야 함.
        #   Phase 2 (Fetch) — 발견된 depth + 원본 params(날짜 포함)로 실제 데이터 조회
        #     Param 엔드포인트 실패 시 statisticsData.do 폴백
        # ═══════════════════════════════════════════════════════════════════

        # ── Phase 1: objL depth + prd_se 자동 발견 ──────────────────────────
        _ep_param   = f"{BASE_URL}/Param/statisticsParameterData.do"
        _ep_bigdata = f"{BASE_URL}/Param/statisticsBigDataParameterData.do"
        _ep_data    = f"{BASE_URL}/statisticsData.do"
        _disc_depth  = 2        # 기본 fallback: 2차원 가정
        _disc_prd    = prd_se
        _disc_found  = False
        _use_bigdata      = False  # err:31 → BigData 엔드포인트 필요 여부
        _use_national_pin = False  # err:31 → objL1=00(전국) 고정 전략 필요 여부

        for _cur_prd in [prd_se] + [p for p in ["Y", "M", "Q"] if p != prd_se]:
            for _d in range(1, 5):
                _disc_p = {
                    "method": "getList", "apiKey": self.api_key,
                    "orgId": org_id, "tblId": tbl_id, "prdSe": _cur_prd,
                    "newEstPrdCnt": "1", "itmId": "ALL",   # ← 날짜 범위 없이 newEstPrdCnt=1
                    "format": "json", "jsonVD": "Y", "errMsg": "Y",
                    **{f"objL{i}": "ALL" for i in range(1, _d + 1)},
                }
                try:
                    _dr = await self._client.get(_ep_param, params=_disc_p, timeout=10.0)
                    _dr.raise_for_status()
                    _dres = _dr.json()
                except Exception:
                    continue
                if isinstance(_dres, list) and _dres:
                    _disc_depth = _d
                    _disc_prd   = _cur_prd
                    _disc_found = True
                    break
                if isinstance(_dres, dict) and _dres.get("err") == "20":
                    continue           # objL 부족 → 다음 차원 추가
                if isinstance(_dres, dict) and _dres.get("err") == "31":
                    # 40,000셀 초과 → ① BigData 엔드포인트 재시도
                    _bd_ok = False
                    try:
                        _bdr = await self._client.get(_ep_bigdata, params=_disc_p, timeout=15.0)
                        _bdr.raise_for_status()
                        _bdres = _bdr.json()
                        if isinstance(_bdres, list) and _bdres:
                            _disc_depth  = _d
                            _disc_prd    = _cur_prd
                            _disc_found  = True
                            _use_bigdata = True
                            _bd_ok = True
                            break
                    except Exception:
                        pass
                    if not _bd_ok:
                        # ② BigData도 err:31 → objL1=00(전국) 전략으로 depth는 확정
                        _disc_p_nat = {**_disc_p, "objL1": "00"}
                        try:
                            _nr = await self._client.get(_ep_param, params=_disc_p_nat, timeout=10.0)
                            _nr.raise_for_status()
                            _nres = _nr.json()
                            if isinstance(_nres, list) and _nres:
                                _disc_depth       = _d
                                _disc_prd         = _cur_prd
                                _disc_found       = True
                                _use_national_pin = True   # Phase 2에서 objL1=00 사용
                                break
                        except Exception:
                            pass
                        # 확인 불가여도 depth는 기록해두고 Phase 2에서 국내 전략 시도
                        if not _disc_found:
                            _disc_depth = _d
                            _disc_prd   = _cur_prd
                break                  # 빈 list, err:31(모두 실패) 등 → 다음 prd_se
            if _disc_found:
                break

        # prd_se 가 바뀌었으면 _build_params 재정의
        if _disc_prd != prd_se:
            def _build_params(**extra) -> dict:  # noqa: F811
                p = {
                    "method": "getList", "apiKey": self.api_key,
                    "orgId": org_id, "tblId": tbl_id, "prdSe": _disc_prd,
                    "format": "json", "jsonVD": "Y", "errMsg": "Y",
                }
                if start_prd_de: p["startPrdDe"] = start_prd_de
                if end_prd_de:   p["endPrdDe"]   = end_prd_de
                if new_est_prd_cnt and not start_prd_de:
                    p["newEstPrdCnt"] = str(new_est_prd_cnt)
                p.update(extra)
                return p

        # ── Phase 2: 발견된 depth + 원본 params 로 실제 데이터 조회 ─────────
        _objl_all = {f"objL{i}": "ALL" for i in range(1, _disc_depth + 1)}
        data = None
        # BigData 엔드포인트가 필요한 표는 BigData를 먼저 시도
        _fetch_eps = ([_ep_bigdata, _ep_param, _ep_data] if _use_bigdata
                      else [_ep_param, _ep_data])
        for _fetch_ep in _fetch_eps:   # BigData 또는 Param → statisticsData.do 폴백
            try:
                _fr = await self._client.get(
                    _fetch_ep, params=_build_params(itmId="ALL", **_objl_all)
                )
                _fr.raise_for_status()
                _fd = _fr.json()
            except Exception:
                continue
            if isinstance(_fd, list):
                data = _fd
                break
            if isinstance(_fd, dict) and _fd.get("err") == "31":
                data = _fd    # err:31 → 아래 축소 재시도 핸들러로 전달
                break
            # err:20 또는 기타 → 다음 엔드포인트 시도
        if data is None:
            data = []

        _found_depth = _disc_depth  # 이후 err:31 재시도에서 사용

        # ── 3. breakdown=False → 조회된 데이터에서 합계코드 추출 후 재조회 ────
        #    (셀 수 절감 선택적 최적화. 실패하면 ALL로 가져온 원본 데이터 그대로 사용)
        if isinstance(data, list) and data and not breakdown and not expand_c1:
            _total_kws = {"계", "합계", "전국", "전체", "소계"}
            _tcodes: dict[str, str] = {}
            _iids: list[str] = []
            _seen_iids: set[str] = set()
            for _row in data:
                _itm = _row.get("ITM_ID", "")
                if _itm and _itm not in _seen_iids:
                    _seen_iids.add(_itm)
                    _iids.append(_itm)
                for _ci in range(1, _found_depth + 1):
                    if str(_ci) not in _tcodes:
                        _nm = _row.get(f"C{_ci}_NM", "")
                        _val = _row.get(f"C{_ci}", "")
                        if _val and _nm in _total_kws:
                            _tcodes[str(_ci)] = _val
            if _tcodes:
                _opt_extra = {f"objL{i}": _tcodes.get(str(i), "ALL") for i in range(1, _found_depth + 1)}
                _opt_extra["itmId"] = "+".join(_iids[:30]) if _iids else "ALL"
                try:
                    _opt_r = await self._client.get(
                        f"{BASE_URL}/Param/statisticsParameterData.do",
                        params=_build_params(**_opt_extra),
                    )
                    _opt_r.raise_for_status()
                    _opt_data = _opt_r.json()
                    if isinstance(_opt_data, list) and _opt_data:
                        data = _opt_data  # 최적화 성공 → 좁은 결과 사용
                    # else: 최적화 실패 → ALL로 가져온 원본 data 유지 (안전)
                except Exception:
                    pass

        # ── 4. err:31: 40,000셀 초과 → 전략적 재시도 ──────────────────────────
        # 전략 순서:
        #   ① objL1=00(전국 합계) 고정  — 지역 차원이 있는 표에서 수백 배 셀 절감
        #   ② objL1=00 + 기간 축소     — 여전히 크면 기간도 줄임
        #   ③ 기간 축소만              — 지역 차원이 없는 표 대비 최후 수단
        _objl_all = {f"objL{i}": "ALL" for i in range(1, _found_depth + 1)}
        _pinned_national = _use_national_pin   # Discovery에서 이미 플래그가 설정됐으면 인계
        retries = 3
        while isinstance(data, dict) and data.get("err") == "31" and retries > 0:
            retries -= 1
            if not _pinned_national:
                # 전략 ①: objL1=00(전국) 고정 + breakdown시 핵심 3지표 좁힘
                # 지역 차원이 있는 표에서 수백 배 셀 절감
                _pinned_national = True
                _objl_nat = {**_objl_all, "objL1": "00"}
                # breakdown=True 시 itmId를 사업체수·종사자수·매출액/출하액으로 좁힘
                # (KOSIS 서비스업 표 공통 코드: T01·T02·T03)
                _itm_narrow = "T01+T02+T03" if breakdown else "ALL"
                reduced = _build_params(**_objl_nat, itmId=_itm_narrow)
            else:
                # 전략 ②③: 기간 축소 (objL1=00 유지)
                if new_est_prd_cnt and new_est_prd_cnt > 1:
                    new_est_prd_cnt = max(1, new_est_prd_cnt // 2)
                elif start_prd_de and end_prd_de:
                    try:
                        s = int(start_prd_de[:4])
                        e = int(end_prd_de[:4])
                        mid = (s + e) // 2
                        start_prd_de = str(mid) + start_prd_de[4:]
                    except Exception:
                        break
                else:
                    break
                _objl_for_retry = {**_objl_all, "objL1": "00"}  # 전국 고정 유지
                _itm_narrow = "T01+T02+T03" if breakdown else "ALL"
                reduced = _build_params(**_objl_for_retry, itmId=_itm_narrow)
                if new_est_prd_cnt:
                    reduced["newEstPrdCnt"] = str(new_est_prd_cnt)
                    reduced.pop("startPrdDe", None)
                    reduced.pop("endPrdDe", None)
            _retry_eps = (
                [_ep_bigdata, _ep_param, _ep_data] if _use_bigdata
                else [_ep_param, _ep_bigdata, _ep_data]
            )
            for ep in _retry_eps:
                try:
                    r = await self._client.get(ep, params=reduced, timeout=30.0)
                    r.raise_for_status()
                    data = r.json()
                    if isinstance(data, list):
                        break
                    if isinstance(data, dict) and data.get("err") != "31":
                        break
                except Exception:
                    continue

        if isinstance(data, dict) and "err" in data:
            raise ValueError(
                f"KOSIS API error: {data}  request_id: {data.get('request_id', '')}"
            )

        # ── 5. breakdown=True 후처리 필터 ─────────────────────────────────────
        # ~3,000행 원본 → ~30행 (전국 + 산업 대분류 + 핵심 지표)
        # - C1_NM = "전국" 행만
        # - C2 코드가 1자 알파벳(KSIC 대분류 A·B·C…) 또는 C2_NM에 "합계" 포함
        # - ITM_NM = 사업체수·종사자수·매출액·출하액 중 하나
        if isinstance(data, list) and data and breakdown and _pinned_national:
            _CORE_ITEMS = {"사업체수", "종사자수", "매출액", "출하액"}
            _filtered = [
                r for r in data
                if r.get("C1_NM") == "전국"
                and (
                    (len(r.get("C2", "")) == 1 and r.get("C2", "").isalpha())
                    or "합계" in r.get("C2_NM", "")
                )
                and r.get("ITM_NM") in _CORE_ITEMS
            ]
            if _filtered:   # 필터 결과가 있을 때만 적용 (없으면 원본 유지)
                data = _filtered

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
            async with _KOSIS_GLOBAL_SEM:
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


    async def search_statistics_global(self, keyword: str) -> list[dict]:
        """vw_cd 없이 KOSIS 전체 카탈로그에서 키워드 검색.
        기관 무관하게 모든 통계표를 대상으로 함."""
        try:
            params = {
                "method": "getList",
                "apiKey": self.api_key,
                "searchNm": keyword,
                "sort": "RANK",
                "startCount": "1",
                "resultCount": "30",
                "format": "json",
                "jsonVD": "Y",
                "errMsg": "Y",
            }
            async with _KOSIS_GLOBAL_SEM:
                resp = await self._client.get(
                    f"{BASE_URL}/statisticsSearch.do", params=params, timeout=30.0
                )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    return data
        except Exception:
            pass
        return []

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

        # 쿼리 토큰 추출 (조사 제거 포함) — 표명 재랭킹에 사용
        raw_tokens = set(query.lower().split())
        query_tokens: set[str] = raw_tokens | {_strip_particle(t) for t in raw_tokens}
        search_tokens = {t for t in query_tokens if len(t) >= 2 and t not in _SEARCH_STOPWORDS}
        # 전체 검색용: 가장 긴 토큰 최대 2개 추출
        global_kws = sorted(search_tokens, key=len, reverse=True)[:2]

        def _to_item(r: dict) -> dict:
            tid = r.get("TBL_ID", "")
            oid = r.get("ORG_ID", "")
            nm  = _normalize_output(r.get("TBL_NM", ""))
            return {
                "org_id": oid,
                "tbl_id": tid,
                "name": nm,
                "updated": r.get("SEND_DE", ""),
                "url": f"https://kosis.kr/statHtml/statHtml.do?orgId={oid}&tblId={tid}",
            }

        # 1차: 인텐트 기반 카테고리 검색 (기존 방식, 정밀도 높음)
        async def search_one(intent_cfg: dict) -> list[dict]:
            found: list[dict] = []
            seen_tbl: set[str] = set()
            for kw in intent_cfg["search_keywords"]:
                try:
                    results = await self.search_statistics(
                        keyword=kw, vw_cd=intent_cfg["vw_cd"]
                    )
                    for r in results:
                        tid = r.get("TBL_ID", "")
                        if tid and tid not in seen_tbl:
                            seen_tbl.add(tid)
                            found.append(_to_item(r))
                except Exception:
                    pass
            return found

        # 2차: 전체 카탈로그 검색 (vw_cd 없음) — 기관 무관 커버리지 확보
        async def search_global(kw: str) -> list[dict]:
            try:
                results = await self.search_statistics_global(keyword=kw)
                return [_to_item(r) for r in results if r.get("TBL_ID")]
            except Exception:
                return []

        # 병렬 실행
        intent_batches, global_batches = await asyncio.gather(
            asyncio.gather(*[search_one(ic) for ic in intents], return_exceptions=True),
            asyncio.gather(*[search_global(kw) for kw in global_kws], return_exceptions=True),
        )

        # 통합 + 표명-쿼리 토큰 겹침 스코어로 재랭킹
        scored: dict[str, tuple[dict, float]] = {}

        for batch in intent_batches:
            if isinstance(batch, list):
                for item in batch:
                    uid = f"{item['org_id']}_{item['tbl_id']}"
                    # 인텐트 결과는 카테고리 정확도 보너스 +0.3
                    score = _score_tbl(item["name"], search_tokens) + 0.3
                    if uid not in scored or scored[uid][1] < score:
                        scored[uid] = (item, score)

        for batch in global_batches:
            if isinstance(batch, list):
                for item in batch:
                    uid = f"{item['org_id']}_{item['tbl_id']}"
                    if uid not in scored or scored[uid][1] < score:
                        scored[uid] = (item, score)

        tables = [
            item for item, _ in
            sorted(scored.values(), key=lambda x: x[1], reverse=True)
        ][:max_results]

        return {
            "query": query,
            "intents": [i["intent"] for i in intents],
            "count": len(tables),
            "tables": tables,
        }
