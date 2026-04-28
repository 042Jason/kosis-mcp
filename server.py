"""
KOSIS MCP Server - FastMCP + Streamable HTTP (MCP 2025-03-26)
"""

import asyncio
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


_KEEP_FIELDS = {"PRD_DE", "DT", "ITM_NM", "C1_NM", "C2_NM", "C3_NM", "UNIT_NM"}


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
    <p style="font-size:.92rem;color:#475569;margin-bottom:12px">Claude 앱 → 설정(Settings) → 통합(Integrations) → 사용자 통합 추가(Add custom integration)</p>
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
    [query 작성 규칙] 사용자 질문에서 핵심 주제어만 추출해 전달하라 (예: "자살률" "고령화" "청년실업").
    '연령별', '성별', '지역별', '월별' 같은 차원·분류어는 query에 포함하지 말 것 — 검색 노이즈가 된다.
    [출력 규칙] 사용자에게 결과를 안내할 때는 반드시 각 항목의 'name' 필드(통계표명)를 사용하라.
    'tbl_id'(예: DT_1B34E01) 같은 내부 식별자는 사용자에게 노출하지 말 것 — kosis_analyze 호출 시에만 내부적으로 사용.
    [URL 표시 규칙] 사용자에게 결과를 안내할 때 각 항목의 'url' 필드를 함께 표시하라 — 사용자가 KOSIS에서 직접 확인할 수 있도록."""
    client = _get_client()
    result = await client.search_by_intent(query=query, max_results=max_results)
    result["source"] = "국가데이터처 KOSIS"
    # 각 결과 항목에 KOSIS 직접 접근 URL 추가
    for item in result.get("tables", []):
        oid = item.get("org_id", "")
        tid = item.get("tbl_id", "")
        if oid and tid:
            item["url"] = f"https://kosis.kr/statHtml/statHtml.do?orgId={oid}&tblId={tid}"
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def kosis_analyze(
    org_id: str,
    tbl_id: str,
    title: str,
    chart_type: str = "line",
    start_year: str = "",
    end_year: str = "",
    recent_n: int = 20,
    prd_se: str = "Y",
    color_field: str = "",
    filter_keyword: str = "",
    breakdown: bool = False,
) -> str:
    """KOSIS 통계표 데이터를 조회하고 chart_hint와 함께 반환합니다.
    출처는 항상 '국가데이터처 KOSIS'로 표기할 것 (구 통계청 — 2025년 국가데이터처로 기관명 변경).
    filter_keyword: 특정 항목만 필터링. 공백 구분 시 모든 단어를 AND 조건으로 매칭.
      예) "자살" → 자살 포함 행만 / "대전 서구" → 대전+서구 모두 포함 행만 (전국 서구 중복 해소).
      지역명 중복이 있는 경우 반드시 상위+하위 지역명을 함께 입력하라 (예: "부산 중구", "서울 중구").
    breakdown: False(기본)=집계 합계만 조회(셀 수 최소화), True=성별·연령별 등 전체 세분류 조회(셀 수 증가 주의).
    [출처 표시 — 필수, 예외 없음] 이 도구를 호출한 결과를 사용자에게 전달할 때는 출력 형식과 무관하게 항상 아래 규칙을 따르라.
    - 텍스트·요약·분석: 응답 마지막에 반드시 "출처: 국가데이터처 KOSIS 「{title}」 {url}" 형식으로 표시.
    - 표(table): 표 하단에 반드시 'citation' + 'url' 표시.
    - 차트·대시보드·시각화: 반드시 'citation' 필드 값을 footer에 표시 (URL은 시각화 내부엔 생략).
    org_id·tbl_id 같은 내부 식별자는 사용자에게 노출하지 말 것."""
    client = _get_client()
    data = await client.get_statistics_data(
        org_id=org_id, tbl_id=tbl_id, prd_se=prd_se,
        start_prd_de=start_year or None,
        end_prd_de=end_year or None,
        new_est_prd_cnt=recent_n,
        breakdown=breakdown,
        expand_c1=bool(filter_keyword),  # 필터 지정 시 C1 차원 전체 펼침
    )
    if not data:
        return "데이터가 없습니다."
    cf = color_field or None
    if not cf:
        for c in ("ITM_NM", "C1_NM", "C2_NM"):
            if c in set(data[0].keys()):
                cf = c
                break
    rows, summary, unit = _process_data(data, cf)
    # filter_keyword 필터링 — 공백 구분 시 모든 단어 AND 매칭 (예: "대전 서구" → 대전 AND 서구)
    if filter_keyword:
        filter_cols = [k for k in (rows[0].keys() if rows else [])
                       if k.endswith("_NM") or k == "ITM_NM"]
        terms = [t.lower() for t in filter_keyword.split() if t]
        rows = [r for r in rows
                if all(any(t in str(r.get(c, "")).lower() for c in filter_cols) for t in terms)]
    return json.dumps({
        "title": title, "org_id": org_id, "tbl_id": tbl_id,
        "unit": unit, "rows": len(rows), "summary": summary,
        "source": "국가데이터처 KOSIS",
        "citation": f"출처: 국가데이터처 KOSIS 「{title}」",
        "url": f"https://kosis.kr/statHtml/statHtml.do?orgId={org_id}&tblId={tbl_id}",
        "chart_hint": {"chart_type": chart_type, "x_field": "PRD_DE", "y_field": "DT", "color_field": cf},
        "data": rows[:100],
    }, ensure_ascii=False, indent=2)


@mcp.tool()
async def kosis_browse(vw_cd: str = "MT_ZTITLE", parent_list_id: str = "A") -> str:
    """KOSIS 카테고리 트리를 탐색합니다. vw_cd: MT_ZTITLE(주제별) MT_TM1_TITLE(대상별) MT_TM2_TITLE(이슈별)"""
    client = _get_client()
    result = await client.browse_categories(vw_cd=vw_cd, parent_list_id=parent_list_id)
    tables = [{"org_id": r.get("ORG_ID"), "tbl_id": r.get("TBL_ID"),
               "name": r.get("TBL_NM"), "updated": r.get("SEND_DE")}
              for r in result if r.get("TBL_ID")]
    cats = [{"list_id": r.get("LIST_ID"), "name": r.get("LIST_NM")}
            for r in result if r.get("LIST_ID") and not r.get("TBL_ID")]
    return json.dumps({"sub_categories": cats, "tables": tables[:30]}, ensure_ascii=False, indent=2)


@mcp.tool()
async def kosis_explain(org_id: str, tbl_id: str) -> str:
    """통계표의 조사 목적, 주기, 대상범위 등 메타데이터를 조회합니다."""
    client = _get_client()
    data = await client.get_statistics_explanation(org_id=org_id, tbl_id=tbl_id)
    key_fields = {"TBL_NM", "STAT_NM", "CYCLE", "SURVEY_PURPOSE", "SURVEY_RANGE", "CONTACT_ORG"}
    compact = [{k: v for k, v in row.items() if k in key_fields or not k.endswith("_CD")} for row in data]
    return json.dumps(compact[:5], ensure_ascii=False, indent=2)


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
                new_est_prd_cnt=20,
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
            return {
                "title": ds_cfg["title"], "org_id": ds_cfg["org_id"], "tbl_id": ds_cfg["tbl_id"],
                "unit": unit, "rows": len(rows), "summary": summary,
                "chart_hint": {"chart_type": ds_cfg.get("chart_type", "line"),
                               "x_field": "PRD_DE", "y_field": "DT", "color_field": cf},
                "sample": rows[:5],
            }
        except Exception as e:
            return {"title": ds_cfg.get("title", ""), "error": str(e)}

    fetched = await asyncio.gather(*[fetch_ds(ds) for ds in datasets])
    return json.dumps({
        "dashboard_title": dashboard_title,
        "count": len([f for f in fetched if f]),
        "datasets": [f for f in fetched if f],
    }, ensure_ascii=False, indent=2)


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
