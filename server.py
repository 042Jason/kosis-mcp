"""
KOSIS MCP 서버 — SSE / HTTP transport (클라우드 배포용)

로컬(stdio) 대신 SSE(Server-Sent Events)로 동작하므로
Railway · Render · fly.io 등 어느 클라우드에나 배포할 수 있습니다.

접속 URL 형식:
    https://your-server.com/sse?kosis_key=발급받은_인증키

각 사용자가 자신의 KOSIS 인증키를 URL 파라미터로 전달하므로
서버에 키를 저장하지 않아도 됩니다.

실행:
    pip install -r requirements.txt
    uvicorn server:starlette_app --host 0.0.0.0 --port 8000

환경변수 (선택 — 기본 키 설정 시):
    KOSIS_API_KEY   발급받은 KOSIS 인증키 (URL 파라미터 없을 때 폴백)
    KOSIS_OUTPUT_DIR  차트 저장 경로 (기본: ~/kosis_charts)
    PORT            포트 번호 (기본: 8000, Railway/Render 자동 주입)
"""

import asyncio
import contextvars
import json
import os
from pathlib import Path

import uvicorn
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp import types
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Mount, Route

from kosis_client import KosisClient
from visualizer import create_chart, create_dashboard, CHART_TYPES

# ── 설정 ──────────────────────────────────────────────────────────────────────
DEFAULT_API_KEY = os.environ.get("KOSIS_API_KEY", "")
OUTPUT_DIR = os.environ.get("KOSIS_OUTPUT_DIR", str(Path.home() / "kosis_charts"))
PORT = int(os.environ.get("PORT", 8000))

# 접속별 API 키를 담는 컨텍스트 변수 (스레드 안전)
_api_key_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "kosis_api_key", default=DEFAULT_API_KEY
)


def _get_client() -> KosisClient:
    key = _api_key_ctx.get()
    if not key:
        raise ValueError(
            "KOSIS 인증키가 없습니다. "
            "접속 URL에 ?kosis_key=YOUR_KEY 를 추가하거나 "
            "서버의 KOSIS_API_KEY 환경변수를 설정하세요."
        )
    return KosisClient(key)


# ── MCP 서버 ──────────────────────────────────────────────────────────────────
mcp_app = Server("kosis-research-mcp")


@mcp_app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="kosis_browse_categories",
            description=(
                "KOSIS 통계 카테고리 트리를 탐색합니다.\n"
                "최상위(parent_list_id='A')부터 시작해 관련 분류로 좁혀 나가며\n"
                "원하는 통계표의 ORG_ID·TBL_ID를 찾을 수 있습니다.\n\n"
                "vw_cd 주요 값:\n"
                "  MT_ZTITLE  – 국내통계 주제별\n"
                "  MT_OTITLE  – 기관별\n"
                "  MT_RTITLE  – 국제통계\n"
                "  MT_BUKHAN  – 북한통계\n"
                "  MT_TM1_TITLE – 대상별 (아동·고령자·여성 등)\n"
                "  MT_TM2_TITLE – 이슈별 (저출산·고령화·지역균형 등)"
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
            name="kosis_search_statistics",
            description=(
                "키워드로 KOSIS 통계표를 검색합니다.\n"
                "사용자 의도에서 핵심 키워드를 추출해 호출하세요.\n\n"
                "예시:\n"
                "  '인구 소멸' → ['인구','출생','합계출산율','고령화']\n"
                "  '청년 실업' → ['청년','실업','고용','취업']"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string"},
                    "vw_cd": {"type": "string", "default": "MT_ZTITLE"},
                },
                "required": ["keyword"],
            },
        ),
        types.Tool(
            name="kosis_get_statistics_data",
            description=(
                "KOSIS 통계표의 실제 수치 데이터를 조회합니다.\n\n"
                "prd_se: Y=연, M=월, Q=분기, H=반기, D=일\n"
                "obj_l1·itm_id = 'ALL' 이면 전체 반환"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "org_id": {"type": "string"},
                    "tbl_id": {"type": "string"},
                    "obj_l1": {"type": "string", "default": "ALL"},
                    "itm_id": {"type": "string", "default": "ALL"},
                    "prd_se": {"type": "string", "default": "Y"},
                    "start_prd_de": {"type": "string"},
                    "end_prd_de": {"type": "string"},
                    "new_est_prd_cnt": {"type": "integer", "default": 15},
                },
                "required": ["org_id", "tbl_id"],
            },
        ),
        types.Tool(
            name="kosis_get_explanation",
            description="통계표의 조사 설명(목적·주기·대상범위 등)을 조회합니다.",
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
            name="kosis_create_chart",
            description=(
                "KOSIS 통계 데이터로 차트를 생성합니다.\n\n"
                "chart_type:\n"
                "  line       – 시계열 추이\n"
                "  multi_line – 복수 분류 비교\n"
                "  bar        – 세로 막대\n"
                "  bar_h      – 가로 막대 (지역 비교)\n"
                "  area       – 면적형\n"
                "  pie        – 구성비\n"
                "  scatter    – 산점도"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "data_json": {"type": "string"},
                    "chart_type": {"type": "string", "enum": list(CHART_TYPES)},
                    "title": {"type": "string"},
                    "x_field": {"type": "string", "default": "PRD_DE"},
                    "y_field": {"type": "string", "default": "DT"},
                    "color_field": {"type": "string"},
                    "save_file": {"type": "boolean", "default": True},
                },
                "required": ["data_json", "chart_type", "title"],
            },
        ),
        types.Tool(
            name="kosis_create_dashboard",
            description="복수의 통계 데이터셋을 하나의 대시보드로 시각화합니다.",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasets": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "data_json": {"type": "string"},
                                "chart_type": {"type": "string"},
                                "title": {"type": "string"},
                                "x_field": {"type": "string"},
                                "y_field": {"type": "string"},
                                "color_field": {"type": "string"},
                            },
                            "required": ["data_json", "title"],
                        },
                    },
                    "dashboard_title": {"type": "string", "default": "KOSIS 통계 대시보드"},
                },
                "required": ["datasets"],
            },
        ),
    ]


@mcp_app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent | types.ImageContent]:
    client = _get_client()

    if name == "kosis_browse_categories":
        result = await client.browse_categories(
            vw_cd=arguments.get("vw_cd", "MT_ZTITLE"),
            parent_list_id=arguments.get("parent_list_id", "A"),
        )
        tables = [r for r in result if r.get("TBL_ID")]
        mid_cats = [r for r in result if r.get("LIST_ID") and not r.get("TBL_ID")]
        summary = {
            "total_items": len(result),
            "sub_categories": [{"LIST_ID": c.get("LIST_ID"), "LIST_NM": c.get("LIST_NM")} for c in mid_cats],
            "tables": [{"ORG_ID": t.get("ORG_ID"), "TBL_ID": t.get("TBL_ID"), "TBL_NM": t.get("TBL_NM"), "SEND_DE": t.get("SEND_DE")} for t in tables[:30]],
            "hint": "LIST_ID → parent_list_id에 넣어 탐색 / TBL_ID → kosis_get_statistics_data 호출",
        }
        return [types.TextContent(type="text", text=json.dumps(summary, ensure_ascii=False, indent=2))]

    elif name == "kosis_search_statistics":
        result = await client.search_statistics(
            keyword=arguments["keyword"],
            vw_cd=arguments.get("vw_cd", "MT_ZTITLE"),
        )
        tables = [{"ORG_ID": r.get("ORG_ID"), "TBL_ID": r.get("TBL_ID"), "TBL_NM": r.get("TBL_NM")} for r in result if r.get("TBL_ID")]
        return [types.TextContent(type="text", text=json.dumps({"keyword": arguments["keyword"], "found": len(tables), "tables": tables[:20]}, ensure_ascii=False, indent=2))]

    elif name == "kosis_get_statistics_data":
        data = await client.get_statistics_data(
            org_id=arguments["org_id"],
            tbl_id=arguments["tbl_id"],
            obj_l1=arguments.get("obj_l1", "ALL"),
            itm_id=arguments.get("itm_id", "ALL"),
            prd_se=arguments.get("prd_se", "Y"),
            start_prd_de=arguments.get("start_prd_de"),
            end_prd_de=arguments.get("end_prd_de"),
            new_est_prd_cnt=arguments.get("new_est_prd_cnt", 15),
        )
        columns = list(data[0].keys()) if data else []
        unique_info = {}
        for col in columns:
            if col.endswith("_NM") and col != "TBL_NM":
                vals = list({r.get(col) for r in data if r.get(col)})
                if vals:
                    unique_info[col] = vals[:20]
        output = {
            "total_rows": len(data),
            "columns": columns,
            "sample_rows": data[:3],
            "unique_values": unique_info,
            "data_json": json.dumps(data, ensure_ascii=False),
            "hint": "data_json을 kosis_create_chart에 그대로 전달하세요.",
        }
        return [types.TextContent(type="text", text=json.dumps(output, ensure_ascii=False, indent=2))]

    elif name == "kosis_get_explanation":
        data = await client.get_statistics_explanation(
            org_id=arguments["org_id"],
            tbl_id=arguments["tbl_id"],
        )
        return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False, indent=2))]

    elif name == "kosis_create_chart":
        result = create_chart(
            data=arguments["data_json"],
            chart_type=arguments.get("chart_type", "line"),
            title=arguments["title"],
            x_field=arguments.get("x_field", "PRD_DE"),
            y_field=arguments.get("y_field", "DT"),
            color_field=arguments.get("color_field"),
            output_dir=OUTPUT_DIR if arguments.get("save_file", True) else None,
            file_stem=arguments["title"].replace(" ", "_")[:40],
        )
        contents: list = [
            types.TextContent(type="text", text=json.dumps({
                "title": arguments["title"],
                "summary": result.get("summary"),
                "html_path": result.get("html_path"),
                "png_path": result.get("png_path"),
            }, ensure_ascii=False, indent=2))
        ]
        if result.get("base64_png"):
            contents.append(types.ImageContent(type="image", data=result["base64_png"], mimeType="image/png"))
        return contents

    elif name == "kosis_create_dashboard":
        raw = arguments["datasets"]
        datasets = []
        for ds in raw:
            item = dict(ds)
            if isinstance(item.get("data_json"), str):
                item["data"] = json.loads(item.pop("data_json"))
            datasets.append(item)
        result = create_dashboard(
            datasets=datasets,
            output_dir=OUTPUT_DIR,
            file_stem=arguments.get("dashboard_title", "dashboard").replace(" ", "_")[:40],
        )
        contents: list = [
            types.TextContent(type="text", text=json.dumps({"html_path": result.get("html_path")}, ensure_ascii=False, indent=2))
        ]
        if result.get("base64_png"):
            contents.append(types.ImageContent(type="image", data=result["base64_png"], mimeType="image/png"))
        return contents

    raise ValueError(f"알 수 없는 도구: {name}")


# ── SSE transport 및 Starlette 앱 ─────────────────────────────────────────────
sse_transport = SseServerTransport("/messages/")


async def handle_sse(request: Request):
    """SSE 연결 엔드포인트. URL 파라미터 kosis_key로 인증키를 받습니다."""
    api_key = request.query_params.get("kosis_key", "") or DEFAULT_API_KEY
    token = _api_key_ctx.set(api_key)
    try:
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await mcp_app.run(
                streams[0], streams[1], mcp_app.create_initialization_options()
            )
    finally:
        _api_key_ctx.reset(token)


async def handle_health(request: Request):
    return JSONResponse({"status": "ok", "server": "kosis-mcp"})


async def handle_index(request: Request):
    """사용자 안내 페이지"""
    host = request.headers.get("host", f"localhost:{PORT}")
    scheme = "https" if request.headers.get("x-forwarded-proto") == "https" else "http"
    base_url = f"{scheme}://{host}"
    sse_url = f"{base_url}/sse?kosis_key=YOUR_KOSIS_API_KEY"

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KOSIS MCP 서버</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 800px; margin: 60px auto; padding: 0 24px; color: #1a1a1a; }}
  h1 {{ font-size: 1.8rem; margin-bottom: 4px; }}
  .badge {{ display: inline-block; background: #22c55e; color: white;
            font-size: 0.75rem; padding: 2px 10px; border-radius: 99px;
            vertical-align: middle; margin-left: 8px; }}
  .section {{ background: #f8fafc; border: 1px solid #e2e8f0;
              border-radius: 12px; padding: 24px; margin: 20px 0; }}
  .section h2 {{ margin-top: 0; font-size: 1rem; color: #475569; text-transform: uppercase;
                 letter-spacing: .05em; }}
  code {{ background: #1e293b; color: #7dd3fc; padding: 12px 16px;
          border-radius: 8px; display: block; font-size: 0.88rem;
          word-break: break-all; line-height: 1.6; white-space: pre-wrap; }}
  input {{ width: 100%; box-sizing: border-box; padding: 10px 14px;
           border: 1px solid #cbd5e1; border-radius: 8px; font-size: 0.95rem; }}
  button {{ margin-top: 10px; padding: 10px 22px; background: #3b82f6;
            color: white; border: none; border-radius: 8px; cursor: pointer;
            font-size: 0.95rem; }}
  button:hover {{ background: #2563eb; }}
  #url-output {{ margin-top: 14px; }}
  ol li {{ margin-bottom: 8px; line-height: 1.7; }}
  a {{ color: #3b82f6; }}
</style>
</head>
<body>
<h1>📊 KOSIS MCP 서버 <span class="badge">Running</span></h1>
<p>KOSIS 국가통계포털 데이터를 AI가 검색·분석·시각화해주는 MCP 서버입니다.</p>

<div class="section">
  <h2>🔑 Step 1 — KOSIS 인증키 발급</h2>
  <ol>
    <li><a href="https://kosis.kr/openapi/" target="_blank">kosis.kr/openapi</a> 접속 후 회원가입</li>
    <li>상단 메뉴 <strong>활용신청</strong> → 인증키 발급 신청</li>
    <li>발급된 키를 아래 입력란에 붙여넣으세요</li>
  </ol>
</div>

<div class="section">
  <h2>🔗 Step 2 — 접속 URL 생성</h2>
  <input id="api-key-input" type="text" placeholder="발급받은 KOSIS 인증키를 여기에 입력하세요" />
  <button onclick="generateUrl()">URL 생성</button>
  <div id="url-output"></div>
</div>

<div class="section">
  <h2>⚙️ Step 3 — Claude Desktop 등록</h2>
  <p>생성된 URL을 <code>claude_desktop_config.json</code>에 추가하세요:</p>
  <code id="config-snippet">{{
  "mcpServers": {{
    "kosis": {{
      "command": "npx",
      "args": ["-y", "mcp-remote", "{sse_url}"]
    }}
  }}
}}</code>
  <p style="margin-top:12px; color:#64748b; font-size:0.88rem;">
    config 파일 위치: Windows → <code style="display:inline; padding: 2px 6px;">%APPDATA%\Claude\claude_desktop_config.json</code>
    &nbsp;|&nbsp; macOS → <code style="display:inline; padding: 2px 6px;">~/Library/Application Support/Claude/claude_desktop_config.json</code>
  </p>
</div>

<div class="section">
  <h2>💬 사용 예시</h2>
  <p>Claude에게 이렇게 말해보세요:</p>
  <ul>
    <li>"인구 소멸 논문을 쓰고 있는데 관련 통계 찾아서 그래프로 정리해줘"</li>
    <li>"최근 10년간 합계출산율 추이를 꺾은선 그래프로 보여줘"</li>
    <li>"지역별 고령화율 비교 차트 만들어줘"</li>
  </ul>
</div>

<script>
function generateUrl() {{
  const key = document.getElementById('api-key-input').value.trim();
  if (!key) {{ alert('인증키를 입력해주세요'); return; }}
  const url = `{base_url}/sse?kosis_key=${{key}}`;
  const config = `{{
  "mcpServers": {{
    "kosis": {{
      "command": "npx",
      "args": ["-y", "mcp-remote", "${{url}}"]
    }}
  }}
}}`;
  document.getElementById('url-output').innerHTML =
    '<p style="margin:10px 0 4px; font-weight:600;">접속 URL:</p><code>' + url + '</code>';
  document.getElementById('config-snippet').textContent = config;
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)


# ── Starlette 앱 조립 ─────────────────────────────────────────────────────────
starlette_app = Starlette(
    routes=[
        Route("/", endpoint=handle_index),
        Route("/health", endpoint=handle_health),
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse_transport.handle_post_message),
    ]
)

# CORS — Claude Desktop 및 claude.ai 에서 접속 허용
starlette_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── 직접 실행 ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 KOSIS MCP 서버 시작 → http://0.0.0.0:{PORT}")
    uvicorn.run(starlette_app, host="0.0.0.0", port=PORT)
