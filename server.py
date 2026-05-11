"""
KOSIS MCP Server - FastMCP + Streamable HTTP (MCP 2025-03-26)
"""

import asyncio
import re
import contextvars
import json
import os

import pandas as pd
import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.types import Receive, Scope, Send

from kosis_client import KosisClient, INTENT_MAP

DEFAULT_API_KEY = os.environ.get("KOSIS_API_KEY", "")
_api_key_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "kosis_api_key", default=DEFAULT_API_KEY
)


def _get_client() -> KosisClient:
    key = _api_key_ctx.get()
    if not key:
        raise ValueError("KOSIS API key missing. Add ?kosis_key=YOUR_KEY to URL.")
    return KosisClient(key)


_KEEP_FIELDS = {"PRD_DE", "DT", "ITM_NM", "C1_NM", "C2_NM", "C3_NM"}
# UNIT_NM은 최상위 unit 필드로 반환 — 각 행 중복 포함 제거로 페이로드 절감


def _process_data(data: list, color_field=None):
    if not data:
        return [], {}, ""
    unit = data[0].get("UNIT_NM", "") or ""
    rows = [{k: v for k, v in row.items() if k in _KEEP_FIELDS} for row in data]
    df = pd.DataFrame(rows)
    if "DT" in df.columns:
        df["DT"] = pd.to_numeric(
            df["DT"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )
    if color_field and color_field in df.columns and df[color_field].nunique() > 12:
        mask = df[color_field].astype(str).str.contains(
            "전국|합계|전체|계$", na=False, regex=True
        )
        if mask.any():
            df = df[mask].copy()
        elif "DT" in df.columns:
            top = df.groupby(color_field)["DT"].mean().dropna().nlargest(10).index
            df = df[df[color_field].isin(top)].copy()
    summary = {}
    if "DT" in df.columns:
        s = df["DT"].dropna()
        if not s.empty:
            trend = (
                "상승" if len(s) >= 2 and float(s.iloc[-1]) > float(s.iloc[0])
                else ("하락" if len(s) >= 2 else "N/A")
            )
            change_pct = None
            if len(s) >= 2 and float(s.iloc[0]) != 0:
                change_pct = round(
                    (float(s.iloc[-1]) - float(s.iloc[0])) / abs(float(s.iloc[0])) * 100, 1
                )
            summary = {
                "count": int(s.count()),
                "min": round(float(s.min()), 3),
                "max": round(float(s.max()), 3),
                "mean": round(float(s.mean()), 3),
                "latest": round(float(s.iloc[-1]), 3),
                "trend": trend,
                "change_pct": change_pct,
            }
    return df.to_dict(orient="records"), summary, unit


# ---------------------------------------------------------------------------
# FastMCP 인스턴스
# ---------------------------------------------------------------------------
# 표 이름에서 수록기간 힌트 파싱용 정규식 (매 요청마다 재컴파일 방지)
_PERIOD_RE = re.compile(r'[(\s](\d{4})[–\-~](\d{4})?')

# ── kosis_quick: 자연어 기간 파싱 ──────────────────────────────────────────
_TIME_RE_PATTERNS = [
    (re.compile(r'(\d+)\s*개?년치'),        lambda m: {"recent_n": int(m.group(1))}),
    (re.compile(r'(\d+)\s*개년'),           lambda m: {"recent_n": int(m.group(1))}),
    (re.compile(r'최근\s*(\d+)\s*개?년'),  lambda m: {"recent_n": int(m.group(1))}),
    (re.compile(r'최근\s*(\d+)\s*개?월'),  lambda m: {"recent_n": int(m.group(1)), "prd_se": "M"}),
    (re.compile(r'(\d{4})\s*년?\s*(?:부터|이후)'), lambda m: {"start_year": m.group(1)}),
    (re.compile(r'(\d{4})\s*[~\-]\s*(\d{4})'),  lambda m: {"start_year": m.group(1), "end_year": m.group(2)}),
]

_QUICK_STOPWORDS = {
    "통계", "자료", "데이터", "줘", "알려줘", "보여줘", "찾아줘", "가져다줘",
    "한국", "대한민국", "전국", "현황", "분석", "조회", "결과", "관련", "좀",
}


def _parse_quick_query(query: str) -> tuple[dict, str]:
    """자연어 쿼리에서 기간 파라미터와 KOSIS 검색 키워드를 분리."""
    time_params: dict = {}
    cleaned = query
    for pattern, extractor in _TIME_RE_PATTERNS:
        m = pattern.search(cleaned)
        if m:
            time_params.update(extractor(m))
            cleaned = pattern.sub("", cleaned).strip()
    tokens = [t for t in cleaned.split() if len(t) >= 2 and t not in _QUICK_STOPWORDS]
    kw = tokens[0] if tokens else cleaned[:8]
    # "프랜차이즈통계" → "프랜차이즈" (접미 '통계' 제거 → KOSIS searchNm 정확도 향상)
    if kw.endswith("통계") and len(kw) > 2:
        kw = kw[:-2]
    return time_params, kw

mcp = FastMCP("kosis-mcp", host="0.0.0.0")


# ---------------------------------------------------------------------------
# Discovery / OAuth 엔드포인트 (Claude가 탐색 시 200 응답)
# ---------------------------------------------------------------------------
def _base_url(request: Request) -> str:
    scheme = "https" if request.headers.get("x-forwarded-proto") == "https" else "http"
    host = request.headers.get("host", "localhost")
    return f"{scheme}://{host}"


@mcp.custom_route("/.well-known/oauth-protected-resource", methods=["GET"])
async def oauth_resource(request: Request) -> Response:
    return JSONResponse({"resource": f"{_base_url(request)}/mcp"},
                        headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/.well-known/oauth-protected-resource/sse", methods=["GET"])
async def oauth_resource_sse(request: Request) -> Response:
    return JSONResponse({"resource": f"{_base_url(request)}/mcp"},
                        headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
async def oauth_auth_server(request: Request) -> Response:
    return JSONResponse({"resource": f"{_base_url(request)}/mcp"},
                        headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/register", methods=["GET", "POST"])
async def register(request: Request) -> Response:
    return JSONResponse({"status": "ok", "service": "kosis-mcp"},
                        headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok", "server": "kosis-mcp"})


@mcp.custom_route("/", methods=["GET"])
async def index(request: Request) -> Response:
    base = _base_url(request)
    mcp_url = f"{base}/mcp?kosis_key=YOUR_KEY"
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>KOSIS MCP</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,sans-serif;background:#f1f5f9;color:#1a1a1a}}
.hero{{background:linear-gradient(135deg,#1e3a5f,#2563eb);color:#fff;padding:56px 24px;text-align:center}}
.hero h1{{font-size:2rem;font-weight:700;margin-bottom:10px}}
.badge{{background:#22c55e;color:#fff;font-size:.72rem;padding:3px 11px;border-radius:99px;margin-left:8px}}
.wrap{{max-width:760px;margin:0 auto;padding:32px 20px}}
.card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:28px;margin-bottom:20px}}
.card h2{{font-size:1rem;font-weight:700;margin-bottom:14px}}
code{{background:#0f172a;color:#7dd3fc;padding:13px 16px;border-radius:9px;display:block;font-size:.84rem;word-break:break-all}}
input{{width:100%;padding:11px 14px;border:1px solid #cbd5e1;border-radius:9px;font-size:.95rem;margin-bottom:8px}}
.btn{{padding:11px 26px;background:#2563eb;color:#fff;border:none;border-radius:9px;cursor:pointer;font-size:.95rem;font-weight:600}}
#url-out{{margin-top:12px}}
a{{color:#2563eb}}
.footer{{text-align:center;padding:24px;font-size:.83rem;color:#94a3b8}}
</style>
</head>
<body>
<div class="hero">
  <h1>KOSIS MCP <span class="badge">Running</span></h1>
  <p style="opacity:.85;margin-top:8px">KOSIS 통계 데이터를 Claude가 검색, 분석, 시각화</p>
</div>
<div class="wrap">
  <div class="card">
    <h2>접속 URL 생성</h2>
    <input id="k" type="text" placeholder="KOSIS 인증키 입력 (kosis.kr/openapi)"/>
    <button class="btn" onclick="gen()">생성</button>
    <div id="url-out"></div>
  </div>
  <div class="card">
    <h2>Claude 연결 방법</h2>
    <p style="font-size:.92rem;color:#475569;margin-bottom:12px">
      ① 위 <b>접속 URL 생성</b>에서 인증키를 입력해 URL을 생성하세요.<br>
      ② Claude 앱 → 설정 → 커넥터 → 커스텀 커넥터 추가 → 이름 자유 설정 / URL에 생성된 주소 붙여넣기
    </p>
    <code id="cfg">{mcp_url}</code>
  </div>
</div>
<div class="footer"><a href="https://kosis.kr">국가데이터처 KOSIS</a></div>
<script>
function gen(){{
  var k=document.getElementById('k').value.trim();
  if(!k){{alert('인증키를 입력하세요');return;}}
  var u='{base}/mcp?kosis_key='+k;
  document.getElementById('url-out').innerHTML='<code style="margin-top:8px;background:#0f172a;color:#7dd3fc;padding:13px 16px;border-radius:9px;display:block;word-break:break-all">'+u+'</code>';
  document.getElementById('cfg').textContent=u;
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# MCP 도구 등록
# ---------------------------------------------------------------------------
@mcp.tool()
async def kosis_find_by_intent(query: str, max_results: int = 12) -> str:
    """사용자의 연구/정책 의도를 자연어로 입력하면 관련 KOSIS 통계표를 자동으로 찾아줍니다.
    데이터 출처: 국가데이터처 KOSIS (구 통계청 — 2025년 국가데이터처로 기관명 변경, 항상 '국가데이터처'로 표기).

    [사용 적합 쿼리] 정책 주제어 · 일반 개념어 (예: "자살률", "고령화", "청년실업", "소비자물가").
    [사용 부적합 쿼리 — 즉시 kosis_browse로 전환]
      - 특정 통계조사 고유명사: "광업제조업조사", "서비스업조사", "기업활동조사", "농업총조사" 등
        → 고유명사는 인텐트 매핑 적중률이 낮음. kosis_browse L_5·O_8 등 카테고리 직접 탐색이 훨씬 정확.
      - 결과가 1회 반환 후 관련 표가 없는 경우 → 재검색하지 말고 바로 kosis_browse로 전환.

    [query 작성 규칙] 핵심 주제어만 추출 (예: "자살률" "고령화" "청년실업").
    '연령별', '성별', '지역별', '월별' 같은 차원·분류어는 포함하지 말 것 — 검색 노이즈.

    [출력 규칙] 결과를 안내할 때 'name' 필드(통계표명)와 'url' 필드를 함께 표시.
    'tbl_id' 같은 내부 식별자는 사용자에게 노출하지 말 것.
    [재검색 금지] 1회 호출로 충분. 관련 표 발견 시 즉시 kosis_analyze 호출."""
    client = _get_client()
    result = await client.search_by_intent(query=query, max_results=max_results)
    result["source"] = "국가데이터처 KOSIS"
    for item in result.get("tables", []):
        oid = item.get("org_id", "")
        tid = item.get("tbl_id", "")
        if oid and tid:
            item["url"] = f"https://kosis.kr/statHtml/statHtml.do?orgId={oid}&tblId={tid}"
        # 표 이름에서 수록기간 힌트 파싱 — Claude가 기간별 표 선택에 활용
        name = item.get("name", "")
        m = _PERIOD_RE.search(name)
        if m:
            item["period_hint"] = f"{m.group(1)}~{m.group(2) or ''}"
    # 동일 기관에서 period_hint가 여러 개면 split 시리즈 알림
    period_items = [t for t in result.get("tables", []) if t.get("period_hint")]
    if len(period_items) >= 2:
        result["split_series_note"] = (
            "동일 통계가 기간별로 분리된 표로 구성됩니다. "
            "연속 시계열 필요 시 kosis_analyze의 extra_tbl_ids에 이전 기간 표 ID를 전달하세요."
        )
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'))


@mcp.tool()
async def kosis_analyze(
    org_id: str,
    tbl_id: str,
    title: str,
    chart_type: str = "line",
    start_year: str = "",
    end_year: str = "",
    recent_n: int = 10,
    prd_se: str = "Y",
    color_field: str = "",
    filter_keyword: str = "",
    breakdown: bool = False,
    extra_tbl_ids: str = "",
) -> str:
    """KOSIS 통계표 데이터를 조회하고 chart_hint와 함께 반환합니다.
    출처는 항상 '국가데이터처 KOSIS'로 표기할 것 (구 통계청 — 2025년 국가데이터처로 기관명 변경).

    [breakdown 파라미터 — 핵심]
    breakdown=True: 산업분류·지역 등 전체 세분류를 펼쳐서 반환. 산업/지역 차원이 있는 표는
      반드시 breakdown=True로 호출해야 함 — False(기본값)는 산업·지역 차원 표에서 빈 결과 반환.
      첫 호출은 filter_keyword를 비워두고 데이터 구조를 먼저 파악한 뒤 필요 시 좁힐 것.
    breakdown=True 결과 필터링 기준:
      - C1_NM(지역) = "전국" 행만 추출
      - C2_NM(산업) = 최상위 집계 항목("제조업 전체", "전산업", "합계" 등) 행만 추출
      - ITM_NM에서 사업체수·종사자수·매출액(또는 출하액) 항목만 선택

    [조사별 주의사항]
    - 광업·제조업조사: 헤드라인 지표는 '매출액'이 아닌 '출하액'. 10인 이상 사업체만 조사 대상.
    - 서비스업조사(도소매·숙박·음식 포함): 매출액이 표준 지표. 1인 이상 사업체 대상.
    - 기업활동조사: 상용 50인 이상 + 자본금 3억 이상만 대상 — 전수통계 아님.
    - 농림어업총조사: 가구 단위 조사라 사업체수·매출액 프레임 부적합. 다른 통계 대안 제안.

    filter_keyword: 특정 항목만 필터링. 공백 구분 시 모든 단어를 AND 조건으로 매칭.
      예) "전국" → 전국 행만 / "대전 서구" → 대전+서구 모두 포함 행만.
      지역명 중복 시 상위+하위 지역명 함께 입력 (예: "부산 중구", "서울 중구").

    extra_tbl_ids: 작성방식 변경으로 시계열이 여러 표로 분리된 경우 이전 표 ID를 쉼표로 구분해 전달.
      예) "DT_3KB9001_OLD,DT_3KB9001_V2"
      [사용 시점] kosis_browse에서 '(YYYY~)' 패턴 형제 카테고리 발견 시, 또는 분류 개정으로
      표가 분리된 경우 최신 표 + 이전 표를 함께 전달해 연속 시계열 구성.

    [출처 표시 — 필수]
    - 텍스트·요약·분석·표: 'citation_full' 필드를 그대로 출력 (URL 포함).
    - 차트·대시보드: 'citation' 필드만 footer에 표시.
    org_id·tbl_id 같은 내부 식별자는 사용자에게 노출하지 말 것."""
    client = _get_client()

    # 주 표 + extra 표 병렬 조회
    extra_ids = [eid.strip() for eid in extra_tbl_ids.split(",") if extra_tbl_ids and eid.strip()]
    all_table_pairs = [(org_id, tbl_id)] + [(org_id, eid) for eid in extra_ids]

    async def _fetch(oid: str, tid: str) -> list:
        try:
            return await client.get_statistics_data(
                org_id=oid, tbl_id=tid, prd_se=prd_se,
                start_prd_de=start_year or None,
                end_prd_de=end_year or None,
                new_est_prd_cnt=recent_n,
                breakdown=breakdown,
                expand_c1=bool(filter_keyword),
            )
        except Exception:
            return []

    all_raw = await asyncio.gather(*[_fetch(oid, tid) for oid, tid in all_table_pairs])

    # 시계열 병합: 주 표(index=0) 데이터의 PRD_DE 집합을 먼저 등록 후 extra는 보완
    primary_data = list(all_raw[0]) if all_raw else []
    extra_data_list = list(all_raw[1:]) if len(all_raw) > 1 else []

    if extra_data_list:
        # PRD_DE를 기준으로 주 표에 없는 기간만 extra에서 추가
        # (완전 중복 제거: 동일 PRD_DE+ITM_NM+C1_NM 조합)
        primary_keys: set[str] = set()
        for row in primary_data:
            key = f"{row.get('PRD_DE','')}__{row.get('ITM_NM','')}__{row.get('C1_NM','')}"
            primary_keys.add(key)

        supplemental: list[dict] = []
        for extra_data in extra_data_list:
            for row in (extra_data or []):
                key = f"{row.get('PRD_DE','')}__{row.get('ITM_NM','')}__{row.get('C1_NM','')}"
                if key not in primary_keys:
                    supplemental.append(row)
                    primary_keys.add(key)

        merged_raw = supplemental + primary_data  # extra(구 데이터)가 앞에 → 정렬 후 연속 시계열
        merged_raw.sort(key=lambda r: r.get("PRD_DE", ""))
    else:
        merged_raw = primary_data

    if not merged_raw:
        # 데이터가 없을 때 명시적 안내 반환 (무관한 표 자동병합 금지)
        hint = ""
        if start_year or end_year:
            hint = (
                f" 요청 기간({start_year or ''}~{end_year or ''})에 이 표의 데이터가 없을 수 있습니다. "
                "작성방식 변경으로 기간별 표가 분리된 경우 kosis_browse로 관련 표를 탐색한 뒤 "
                "extra_tbl_ids 파라미터에 이전 표 ID를 전달하세요."
            )
        return json.dumps({"error": "데이터가 없습니다." + hint, "tbl_id": tbl_id}, ensure_ascii=False)

    # filter_keyword를 _process_data 이전에 원본 데이터에 적용
    # (이후 적용하면 _process_data가 unique>12 행을 "계"만 남겨 filter_keyword가 빈 결과 반환하는 버그 수정)
    if filter_keyword:
        raw_filter_cols = [k for k in (merged_raw[0].keys() if merged_raw else [])
                           if k.endswith("_NM") or k == "ITM_NM"]
        terms = [t.lower() for t in filter_keyword.split() if t]
        merged_raw = [r for r in merged_raw
                      if all(any(t in str(r.get(c, "")).lower() for c in raw_filter_cols)
                             for t in terms)]

    cf = color_field or None
    if not cf:
        for c in ("ITM_NM", "C1_NM", "C2_NM"):
            if c in set(merged_raw[0].keys()) if merged_raw else {}:
                cf = c
                break
    rows, summary, unit = _process_data(merged_raw, cf)

    # 데이터 coverage (시계열 범위) 계산
    prd_values = [r.get("PRD_DE", "") for r in rows if r.get("PRD_DE")]
    coverage = {"from": min(prd_values), "to": max(prd_values)} if prd_values else {}

    merged_info = [tbl_id] + extra_ids if extra_ids else None

    result: dict = {
        "title": title, "unit": unit, "rows": len(rows), "summary": summary,
        "coverage": coverage,
        "source": "국가데이터처 KOSIS",
        "citation": f"출처: 국가데이터처 KOSIS 「{title}」",
        "url": f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}",
        "citation_full": f"출처: 국가데이터처 KOSIS 「{title}」 https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}",
        "chart_hint": {"chart_type": chart_type, "x_field": "PRD_DE", "y_field": "DT", "color_field": cf},
        "data": rows[:60],
    }
    if merged_info:
        result["merged_tables"] = merged_info
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'))


@mcp.tool()
async def kosis_browse(vw_cd: str = "MT_ZTITLE", parent_list_id: str = "A") -> str:
    """KOSIS 카테고리 트리를 탐색합니다. vw_cd: MT_ZTITLE(주제별) MT_TM1_TITLE(대상별) MT_TM2_TITLE(이슈별)

    [워크플로우] 특정 통계조사명(광업제조업조사·서비스업조사 등)은 kosis_find_by_intent보다
    이 도구로 카테고리를 직접 탐색하는 것이 훨씬 정확합니다.

    [주요 카테고리 코드 — MT_ZTITLE 기준]
      L_5  = 광업제조업조사 (사업체수·종사자수·출하액)
      O_8  = 서비스업조사 (도소매·숙박·음식·서비스업 매출액)
      A    = 인구·가구
      C    = 노동·임금
      D    = 농림어업
      F    = 보건·의료
      G    = 환경
      H    = 교육·훈련
      I    = 기업·산업
      J    = 건설·주택
      K    = 교통·물류
      M    = 과학기술·ICT
      N    = 문화·관광
      Q    = 국민계정·경기·기업경영
    모르는 조사: vw_cd=MT_ZTITLE, parent_list_id="A"부터 시작해 알파벳 순서로 탐색.

    [표 선택 기준 — 트리 끝까지 내려간 뒤]
      ① 이름에 "총괄", "주요지표", "산업구조별", "전국" 포함 → 헤드라인 표
      ② "등록기반" vs "조사기반" 중복 시 → 등록기반이 더 포괄적
      ③ "산업편 / 품목편 / 기업체편" 분기 시 → 산업편이 표준
      ④ "11차 산업분류 개정", "10차 산업분류 개정" 식 분리 시 → 최신 표 tbl_id + 이전 표 tbl_id 모두 메모
         → kosis_analyze 호출 시 extra_tbl_ids에 이전 표 전달로 연속 시계열 구성
      ⑤ "종사자규모별", "매출액규모별" 세부 분해 표 → 헤드라인 아님, 건너뜀

    [시계열 분리 감지] sub_categories에 'methodology_split: true' 표시된 항목들은 동일 지표가
    작성방식 변경으로 기간별 분리된 구조입니다. 연속 시계열이 필요하면:
      1. 각 하위 카테고리를 kosis_browse로 탐색해 tbl_id 목록 확인
      2. kosis_analyze 호출 시 최신 표를 tbl_id로, 이전 표들을 extra_tbl_ids로 전달
    """
    client = _get_client()
    result = await client.browse_categories(vw_cd=vw_cd, parent_list_id=parent_list_id)
    tables = [{"org_id": r.get("ORG_ID"), "tbl_id": r.get("TBL_ID"),
               "name": r.get("TBL_NM"), "updated": r.get("SEND_DE")}
              for r in result if r.get("TBL_ID")]
    cats = [{"list_id": r.get("LIST_ID"), "name": r.get("LIST_NM")}
            for r in result if r.get("LIST_ID") and not r.get("TBL_ID")]

    # 방법론 분리 감지: "(YYYY~)" 또는 "YYYY년~" 패턴이 있는 형제 카테고리들
    _year_range_re = re.compile(r'[(\s](\d{4})[년~]')
    year_tagged = [c for c in cats if _year_range_re.search(c.get("name", ""))]
    if len(year_tagged) >= 2:
        for c in year_tagged:
            c["methodology_split"] = True
        split_note = (
            "이 카테고리는 작성방식 변경으로 시계열이 분리되어 있습니다. "
            "연속 시계열 조회 시 각 하위 카테고리를 kosis_browse로 탐색 후 "
            "kosis_analyze의 extra_tbl_ids 파라미터로 이전 표를 병합하세요."
        )
    else:
        split_note = None

    response: dict = {"sub_categories": cats, "tables": tables[:30]}
    if split_note:
        response["split_note"] = split_note
    return json.dumps(response, ensure_ascii=False, separators=(',', ':'))


@mcp.tool()
async def kosis_explain(org_id: str, tbl_id: str) -> str:
    """통계표의 조사 목적, 주기, 대상범위 등 메타데이터를 조회합니다."""
    client = _get_client()
    data = await client.get_statistics_explanation(org_id=org_id, tbl_id=tbl_id)
    key_fields = {"TBL_NM", "STAT_NM", "CYCLE", "SURVEY_PURPOSE", "SURVEY_RANGE", "CONTACT_ORG"}
    compact = [{k: v for k, v in row.items() if k in key_fields or not k.endswith("_CD")} for row in data]
    return json.dumps(compact[:5], ensure_ascii=False, separators=(',', ':'))


# kosis_quick: @mcp.tool() 제거 — _normalize_output 오류 빈발 + browse→analyze 워크플로우로 대체.
# 내부 함수로 보존 (향후 폴백 로직에서 참조 가능).
async def kosis_quick(query: str) -> str:
    """[DEPRECATED — 도구 미노출] 검색+조회 원스텝 처리. _normalize_output 오류 빈발로 비활성화.
    대안: kosis_browse로 카테고리 탐색 → kosis_analyze(breakdown=True) 워크플로우 사용."""
    client = _get_client()

    # 1. 기간 파싱 + 검색 키워드 추출
    time_params, search_kw = _parse_quick_query(query)
    recent_n  = time_params.get("recent_n", 10)
    start_year = time_params.get("start_year", "")
    end_year   = time_params.get("end_year", "")
    prd_se     = time_params.get("prd_se", "Y")

    # 2. KOSIS 검색 (원래 키워드로 먼저, 결과 없으면 "통계" 미제거 원형으로 재시도)
    results = await client.search_statistics(keyword=search_kw)
    if not results:
        # 원형 키워드로 재시도
        raw_tokens = [t for t in query.split() if len(t) >= 2 and t not in _QUICK_STOPWORDS]
        raw_kw = raw_tokens[0] if raw_tokens else search_kw
        if raw_kw != search_kw:
            results = await client.search_statistics(keyword=raw_kw)

    if not results:
        return json.dumps(
            {"error": f"'{search_kw}' 관련 통계표를 찾을 수 없습니다. kosis_find_by_intent로 탐색해보세요."},
            ensure_ascii=False, separators=(',', ':'),
        )

    # 3. 상위 결과에서 데이터 즉시 조회
    top = results[0]
    org_id = top.get("ORG_ID", "")
    tbl_id = top.get("TBL_ID", "")
    tbl_nm = _normalize_output(top.get("TBL_NM", ""))

    try:
        data = await client.get_statistics_data(
            org_id=org_id, tbl_id=tbl_id,
            prd_se=prd_se,
            start_prd_de=start_year or None,
            end_prd_de=end_year or None,
            new_est_prd_cnt=recent_n,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "matched_table": tbl_nm}, ensure_ascii=False, separators=(',', ':'))

    if not data:
        return json.dumps({"error": "데이터가 없습니다.", "matched_table": tbl_nm}, ensure_ascii=False, separators=(',', ':'))

    cf = None
    for c in ("ITM_NM", "C1_NM", "C2_NM"):
        if data and c in data[0]:
            cf = c
            break
    rows, summary, unit = _process_data(data, cf)

    prd_values = [r.get("PRD_DE", "") for r in rows if r.get("PRD_DE")]
    coverage = {"from": min(prd_values), "to": max(prd_values)} if prd_values else {}
    url = f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}"

    # 4. 유사 후보 표 (2위~4위) — 사용자 확인용
    other_candidates = [
        {
            "name": _normalize_output(r.get("TBL_NM", "")),
            "url": f"https://kosis.kr/statHtml/statHtml.do?orgId={r.get('ORG_ID','')}&tblId={r.get('TBL_ID','')}",
        }
        for r in results[1:4] if r.get("TBL_ID")
    ]

    result: dict = {
        "query": query,
        "matched_table": tbl_nm,
        "unit": unit,
        "rows": len(rows),
        "summary": summary,
        "coverage": coverage,
        "source": "국가데이터처 KOSIS",
        "citation": f"출처: 국가데이터처 KOSIS 「{tbl_nm}」",
        "url": url,
        "citation_full": f"출처: 국가데이터처 KOSIS 「{tbl_nm}」 {url}",
        "chart_hint": {"chart_type": "line", "x_field": "PRD_DE", "y_field": "DT", "color_field": cf},
        "data": rows[:60],
    }
    if other_candidates:
        result["other_candidates"] = other_candidates
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'))


@mcp.tool()
async def kosis_combine(
    datasets: list,
    combine_title: str = "통합 분석",
    compute_ratio: bool = True,
    ratio_label: str = "비율(%)",
    join_field: str = "PRD_DE",
) -> str:
    """여러 통계표 데이터를 조회한 뒤 공통 차원(연도 등)으로 JOIN하고 비율을 계산합니다.

    [사용 시점] 단일 표에 '유형별 합계'가 없어서 여러 표를 결합해야 하는 경우.
    예) 노령연금 수급자 표 + 유족연금 수급자 표 + 장애연금 수급자 표 → 유형별 비율 산출.

    [datasets 형식] 각 항목은 dict로 아래 필드를 포함:
      - org_id (str): 기관 ID
      - tbl_id (str): 표 ID
      - label (str): 이 표의 레이블 (예: "노령연금", "유족연금")
      - filter_keyword (str, 선택): 특정 항목 필터 (예: "합계")
      - recent_n (int, 선택, 기본 10): 최근 N년

    [compute_ratio] True이면 각 연도에서 label별 DT 합계를 구하고 비율(%)을 계산해 반환.
    [출처 표시 — 필수] 반환된 citations 배열을 모두 출력할 것."""
    client = _get_client()

    async def fetch_one(ds: dict) -> dict | None:
        org_id = ds.get("org_id", "")
        tbl_id = ds.get("tbl_id", "")
        label  = ds.get("label", tbl_id)
        fk     = ds.get("filter_keyword", "")
        n      = int(ds.get("recent_n", 10))
        try:
            raw = await client.get_statistics_data(
                org_id=org_id, tbl_id=tbl_id,
                prd_se="Y", new_est_prd_cnt=n,
            )
        except Exception as e:
            return {"label": label, "error": str(e)}

        if not raw:
            return {"label": label, "error": "데이터 없음"}

        # filter_keyword 적용 (원본 데이터에서)
        if fk:
            filter_cols = [k for k in raw[0].keys() if k.endswith("_NM") or k == "ITM_NM"]
            terms = [t.lower() for t in fk.split() if t]
            raw = [r for r in raw if all(
                any(t in str(r.get(c, "")).lower() for c in filter_cols)
                for t in terms
            )]

        # 연도별 DT 합산 (숫자형만)
        year_sum: dict[str, float] = {}
        for row in raw:
            yr = str(row.get("PRD_DE", ""))[:4]
            try:
                val = float(row.get("DT", 0) or 0)
            except (ValueError, TypeError):
                continue
            year_sum[yr] = year_sum.get(yr, 0.0) + val

        url = f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}"
        tbl_nm = _normalize_output(raw[0].get("TBL_NM", tbl_id) if raw else tbl_id)
        return {
            "label": label,
            "tbl_nm": tbl_nm,
            "year_sum": year_sum,
            "citation": f"출처: 국가데이터처 KOSIS 「{tbl_nm}」 {url}",
        }

    fetched = await asyncio.gather(*[fetch_one(ds) for ds in datasets])
    valid   = [f for f in fetched if f and "error" not in f]
    errors  = [f for f in fetched if f and "error" in f]

    if not valid:
        return json.dumps({"error": "조회 가능한 데이터가 없습니다.", "details": errors},
                          ensure_ascii=False, separators=(',', ':'))

    # 공통 연도 집합
    all_years = sorted(set().union(*[set(v["year_sum"].keys()) for v in valid]))

    rows: list[dict] = []
    for yr in all_years:
        total = sum(v["year_sum"].get(yr, 0.0) for v in valid)
        row: dict = {join_field: yr, "합계": round(total, 2)}
        for v in valid:
            val = v["year_sum"].get(yr, 0.0)
            row[v["label"]] = round(val, 2)
            if compute_ratio and total > 0:
                row[f'{v["label"]}_{ratio_label}'] = round(val / total * 100, 1)
        rows.append(row)

    citations = [v["citation"] for v in valid]
    labels    = [v["label"] for v in valid]

    return json.dumps({
        "title": combine_title,
        "labels": labels,
        "years": all_years,
        "rows": rows,
        "compute_ratio": compute_ratio,
        "chart_hint": {
            "chart_type": "bar",
            "x_field": join_field,
            "stack": True,
            "series": labels,
        },
        "citations": citations,
        "errors": errors if errors else None,
    }, ensure_ascii=False, separators=(',', ':'))


@mcp.tool()
async def kosis_dashboard(datasets: list, dashboard_title: str = "KOSIS 통계 대시보드") -> str:
    """여러 통계표 데이터를 한꺼번에 조회해 반환합니다."""
    client = _get_client()

    async def fetch_ds(ds_cfg):
        try:
            data = await client.get_statistics_data(
                org_id=ds_cfg["org_id"], tbl_id=ds_cfg["tbl_id"],
                prd_se=ds_cfg.get("prd_se", "Y"),
                start_prd_de=ds_cfg.get("start_year"),
                end_prd_de=ds_cfg.get("end_year"),
                new_est_prd_cnt=10,
            )
            if not data:
                return None
            cf = ds_cfg.get("color_field")
            if not cf:
                for c in ("ITM_NM", "C1_NM", "C2_NM"):
                    if c in set(data[0].keys()):
                        cf = c
                        break
            rows, summary, unit = _process_data(data, cf)
            oid, tid = ds_cfg["org_id"], ds_cfg["tbl_id"]
            return {
                "title": ds_cfg["title"],
                "unit": unit, "rows": len(rows), "summary": summary,
                "citation": f"출처: 국가데이터처 KOSIS 「{ds_cfg['title']}」",
                "chart_hint": {"chart_type": ds_cfg.get("chart_type", "line"),
                               "x_field": "PRD_DE", "y_field": "DT", "color_field": cf},
                # sample 25행: kosis_analyze 추가 호출 없이 차트 생성 가능한 최소 데이터
                "data": rows[:25],
            }
        except Exception as e:
            return {"title": ds_cfg.get("title", ""), "error": str(e)}

    fetched = await asyncio.gather(*[fetch_ds(ds) for ds in datasets])
    return json.dumps({
        "count": len([f for f in fetched if f]),
        "datasets": [f for f in fetched if f],
    }, ensure_ascii=False, separators=(',', ':'))


# ---------------------------------------------------------------------------
# ASGI 앱: FastMCP 앱에 API 키 미들웨어 씌우기
# ---------------------------------------------------------------------------
_fastmcp_app = mcp.streamable_http_app()
_fastmcp_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class _ApiKeyMiddleware:
    """/mcp 와 /sse 요청에서 kosis_key 쿼리 파라미터를 contextvar에 주입."""

    def __init__(self, app):
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path = scope.get("path", "")
            # /sse -> /mcp 리라이트 (하위 호환)
            if path == "/sse":
                scope = dict(scope)
                scope["path"] = "/mcp"
                scope["raw_path"] = b"/mcp"

            from starlette.requests import Request as Req
            req = Req(scope)
            api_key = req.query_params.get("kosis_key", "") or DEFAULT_API_KEY
            token = _api_key_ctx.set(api_key)
            try:
                await self._app(scope, receive, send)
            finally:
                _api_key_ctx.reset(token)
        else:
            await self._app(scope, receive, send)


starlette_app = _ApiKeyMiddleware(_fastmcp_app)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(starlette_app, host="0.0.0.0", port=port, log_level="info")
