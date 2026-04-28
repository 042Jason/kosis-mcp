"""
KOSIS MCP 서버 -- Streamable HTTP transport (MCP 2025-03-26 spec)

접속 URL: https://your-server.com/mcp?kosis_key=발급받은_인증키
"""

import asyncio
import contextvars
import json
import os

import pandas as pd
import uvicorn
from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp import types
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Mount, Route
from starlette.types import Receive, Scope, Send

from kosis_client import KosisClient, INTENT_MAP

# -- 설정 -----------------------------------------------------------------------
DEFAULT_API_KEY = os.environ.get("KOSIS_API_KEY", "")

_api_key_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "kosis_api_key", default=DEFAULT_API_KEY
)


def _get_client() -> KosisClient:
    key = _api_key_ctx.get()
    if not key:
        raise ValueError("KOSIS API key missing. Add ?kosis_key=YOUR_KEY to URL.")
    return KosisClient(key)


# -- 데이터 전처리 ---------------------------------------------------------------
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


# -- MCP 서버 -------------------------------------------------------------------
mcp_server = Server("kosis-mcp")


@mcp_server.list_tools()
async def list_tools() -> list[types.Tool]:
    intent_list = ", ".join(list(INTENT_MAP.keys())[:10]) + " 등"
    return [
        types.Tool(
            name="kosis_find_by_intent",
            description=(
                "사용자의 연구/정책 의도를 자연어로 입력하면 관련 KOSIS 통계표를 자동으로 찾아줍니다.\n"
                f"지원 의도: {intent_list}"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "max_results": {"type": "integer", "default": 12},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="kosis_analyze",
            description=(
                "KOSIS 통계표 데이터를 조회하고 chart_hint와 함께 반환합니다.\n"
                "반환된 data 배열과 chart_hint를 활용해 Claude가 직접 시각화를 생성합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "org_id": {"type": "string"},
                    "tbl_id": {"type": "string"},
                    "title": {"type": "string"},
                    "chart_type": {
                        "type": "string",
                        "enum": ["line", "bar", "bar_h", "pie", "scatter", "area", "multi_line"],
                        "default": "line",
                    },
                    "start_year": {"type": "string"},
                    "end_year": {"type": "string"},
                    "recent_n": {"type": "integer", "default": 20},
                    "prd_se": {"type": "string", "default": "Y"},
                    "color_field": {"type": "string"},
                },
                "required": ["org_id", "tbl_id", "title"],
            },
        ),
        types.Tool(
            name="kosis_browse",
            description=(
                "KOSIS 카테고리 트리를 탐색합니다. org_id/tbl_id를 모를 때 사용.\n"
                "vw_cd: MT_ZTITLE(주제별) MT_TM1_TITLE(대상별) MT_TM2_TITLE(이슈별)"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "vw_cd": {"type": "string", "default": "MT_ZTITLE"},
                    "parent_list_id": {"type": "string", "default": "A"},
                },
            },
        ),
        types.Tool(
            name="kosis_explain",
            description="통계표의 조사 목적·주기·대상범위 등 메타데이터를 조회합니다.",
            inputSchema={
                "type": "object",
                "properties": {
                    "org_id": {"type": "string"},
                    "tbl_id": {"type": "string"},
                },
                "required": ["org_id", "tbl_id"],
            },
        ),
        types.Tool(
            name="kosis_dashboard",
            description="여러 통계표 데이터를 한꺼번에 조회해 반환합니다. Claude가 대시보드로 시각화합니다.",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasets": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "org_id": {"type": "string"},
                                "tbl_id": {"type": "string"},
                                "title": {"type": "string"},
                                "chart_type": {"type": "string", "default": "line"},
                                "start_year": {"type": "string"},
                                "end_year": {"type": "string"},
                                "color_field": {"type": "string"},
                            },
                            "required": ["org_id", "tbl_id", "title"],
                        },
                    },
                    "dashboard_title": {"type": "string", "default": "KOSIS 통계 대시보드"},
                },
                "required": ["datasets"],
            },
        ),
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    client = _get_client()

    if name == "kosis_find_by_intent":
        result = await client.search_by_intent(
            query=arguments["query"],
            max_results=arguments.get("max_results", 12),
        )
        return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "kosis_analyze":
        org_id = arguments["org_id"]
        tbl_id = arguments["tbl_id"]
        title = arguments["title"]
        chart_type = arguments.get("chart_type", "line")
        color_field = arguments.get("color_field")
        prd_se = arguments.get("prd_se", "Y")
        start_year = arguments.get("start_year")
        end_year = arguments.get("end_year")
        recent_n = arguments.get("recent_n", 20)
        data = await client.get_statistics_data(
            org_id=org_id, tbl_id=tbl_id, prd_se=prd_se,
            start_prd_de=start_year, end_prd_de=end_year, new_est_prd_cnt=recent_n,
        )
        if not data:
            return [types.TextContent(type="text", text="데이터가 없습니다.")]
        if not color_field:
            for c in ("ITM_NM", "C1_NM", "C2_NM"):
                if c in set(data[0].keys()):
                    color_field = c
                    break
        rows, summary, unit = _process_data(data, color_field)
        result = {
            "title": title, "org_id": org_id, "tbl_id": tbl_id,
            "unit": unit, "rows": len(rows), "summary": summary,
            "chart_hint": {
                "chart_type": chart_type,
                "x_field": "PRD_DE", "y_field": "DT", "color_field": color_field,
            },
            "data": rows[:300],
        }
        return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "kosis_browse":
        result = await client.browse_categories(
            vw_cd=arguments.get("vw_cd", "MT_ZTITLE"),
            parent_list_id=arguments.get("parent_list_id", "A"),
        )
        tables = [
            {"org_id": r.get("ORG_ID"), "tbl_id": r.get("TBL_ID"),
             "name": r.get("TBL_NM"), "updated": r.get("SEND_DE")}
            for r in result if r.get("TBL_ID")
        ]
        cats = [
            {"list_id": r.get("LIST_ID"), "name": r.get("LIST_NM")}
            for r in result if r.get("LIST_ID") and not r.get("TBL_ID")
        ]
        return [types.TextContent(type="text", text=json.dumps({
            "sub_categories": cats, "tables": tables[:30],
            "tip": "sub_categories의 list_id를 parent_list_id로 넣거나, tables의 org_id+tbl_id로 kosis_analyze 호출",
        }, ensure_ascii=False, indent=2))]

    elif name == "kosis_explain":
        data = await client.get_statistics_explanation(
            org_id=arguments["org_id"], tbl_id=arguments["tbl_id"]
        )
        key_fields = {"TBL_NM", "STAT_NM", "CYCLE", "SURVEY_PURPOSE", "SURVEY_RANGE", "CONTACT_ORG"}
        compact = [
            {k: v for k, v in row.items() if k in key_fields or not k.endswith("_CD")}
            for row in data
        ]
        return [types.TextContent(type="text", text=json.dumps(compact[:5], ensure_ascii=False, indent=2))]

    elif name == "kosis_dashboard":
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
                    "title": ds_cfg["title"],
                    "org_id": ds_cfg["org_id"], "tbl_id": ds_cfg["tbl_id"],
                    "unit": unit, "rows": len(rows), "summary": summary,
                    "chart_hint": {
                        "chart_type": ds_cfg.get("chart_type", "line"),
                        "x_field": "PRD_DE", "y_field": "DT", "color_field": cf,
                    },
                    "data": rows[:150],
                }
            except Exception as e:
                return {"title": ds_cfg.get("title", ""), "error": str(e)}

        fetched = await asyncio.gather(*[fetch_ds(ds) for ds in arguments["datasets"]])
        return [types.TextContent(type="text", text=json.dumps({
            "dashboard_title": arguments.get("dashboard_title", "KOSIS 통계 대시보드"),
            "count": len([f for f in fetched if f]),
            "datasets": [f for f in fetched if f],
        }, ensure_ascii=False, indent=2))]

    raise ValueError(f"Unknown tool: {name}")


# -- Streamable HTTP transport --------------------------------------------------
session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    event_store=None,
    json_response=False,
    stateless=True,
)


class _McpApp:
    """
    /mcp 와 /sse 양쪽에서 요청을 받아 API 키를 contextvar에 설정한 뒤
    StreamableHTTPSessionManager에 위임합니다.

    - GET  /mcp?kosis_key=... → SSE 스트림 (이벤트 수신)
    - POST /mcp?kosis_key=... → JSON 메시지 (도구 호출 등)
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive)
        api_key = request.query_params.get("kosis_key", "") or DEFAULT_API_KEY
        token = _api_key_ctx.set(api_key)
        try:
            await session_manager.handle_request(scope, receive, send)
        finally:
            _api_key_ctx.reset(token)


_mcp_app = _McpApp()


# -- 일반 페이지 핸들러 ----------------------------------------------------------
async def handle_health(request: Request) -> Response:
    return JSONResponse({"status": "ok", "server": "kosis-mcp"})


async def handle_index(request: Request) -> Response:
    host = request.headers.get("host", "localhost")
    scheme = "https" if request.headers.get("x-forwarded-proto") == "https" else "http"
    base_url = f"{scheme}://{host}"
    mcp_url = f"{base_url}/mcp?kosis_key=YOUR_KEY"
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
  <p style="opacity:.85;margin-top:8px">KOSIS 통계 데이터를 Claude가 검색·분석·시각화</p>
</div>
<div class="wrap">
  <div class="card">
    <h2>접속 URL 생성</h2>
    <input id="k" type="text" placeholder="KOSIS 인증키 입력 (kosis.kr/openapi)"/>
    <button class="btn" onclick="gen()">생성 →</button>
    <div id="url-out"></div>
  </div>
  <div class="card">
    <h2>Claude 연결 방법</h2>
    <p style="font-size:.92rem;color:#475569;margin-bottom:12px">Claude 앱 → Settings → Integrations → Add custom integration</p>
    <code id="cfg">{mcp_url}</code>
  </div>
</div>
<div class="footer"><a href="https://kosis.kr">통계청 KOSIS</a></div>
<script>
function gen(){{
  const k=document.getElementById('k').value.trim();
  if(!k)return alert('인증키를 입력하세요');
  const u=`{base_url}/mcp?kosis_key=${{k}}`;
  document.getElementById('url-out').innerHTML='<code style="margin-top:8px;background:#0f172a;color:#7dd3fc;padding:13px 16px;border-radius:9px;display:block;word-break:break-all">'+u+'</code>';
  document.getElementById('cfg').textContent=u;
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)


# -- 전체 ASGI 앱 ---------------------------------------------------------------
class _KosisMcpApp:
    """
    /mcp, /sse → StreamableHTTP MCP 핸들러
    /          → 홈페이지
    /health    → 헬스체크
    그 외      → 404
    """

    def __init__(self) -> None:
        self._starlette = Starlette(
            routes=[
                Route("/", endpoint=handle_index),
                Route("/health", endpoint=handle_health),
            ]
        )
        self._starlette.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if scope["type"] == "http" and path in ("/mcp", "/sse"):
            await _mcp_app(scope, receive, send)
        else:
            await self._starlette(scope, receive, send)


app = _KosisMcpApp()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

