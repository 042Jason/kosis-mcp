"""
KOSIS MCP 서버 (개선판) — SSE / HTTP transport

개선 사항:
  1. kosis_find_by_intent: 자연어 연구 의도 → 관련 통계표 자동 탐색
  2. kosis_analyze: 데이터 조회 + 차트 생성을 한 번에 (토큰 절약)
  3. kosis_get_data: 요약만 반환 (전체 data_json 제거로 토큰 낭비 방지)
  4. 시각화: kaleido 실패 시 HTML base64 자동 fallback

접속 URL:
    https://your-server.com/sse?kosis_key=발급받은_인증키
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

from kosis_client import KosisClient, INTENT_MAP
from visualizer import create_chart, create_dashboard, CHART_TYPES

# ── 설정 ──────────────────────────────────────────────────────────────────────
DEFAULT_API_KEY = os.environ.get("KOSIS_API_KEY", "")
OUTPUT_DIR = os.environ.get("KOSIS_OUTPUT_DIR", str(Path.home() / "kosis_charts"))

_api_key_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "kosis_api_key", default=DEFAULT_API_KEY
)


def _get_client() -> KosisClient:
    key = _api_key_ctx.get()
    if not key:
        raise ValueError("KOSIS 인증키 없음. URL에 ?kosis_key=YOUR_KEY 추가 필요.")
    return KosisClient(key)


# ── MCP 서버 ──────────────────────────────────────────────────────────────────
mcp_app = Server("kosis-mcp")


@mcp_app.list_tools()
async def list_tools() -> list[types.Tool]:
    intent_list = ", ".join(list(INTENT_MAP.keys())[:10]) + " 등"
    return [
        # ── 1. 의도 기반 통계 탐색 (NEW) ────────────────────────────────────
        types.Tool(
            name="kosis_find_by_intent",
            description=(
                "사용자의 연구/정책 의도를 자연어로 입력하면 관련 KOSIS 통계표를 자동으로 찾아줍니다.\n"
                "단순 키워드 검색과 달리 의도를 분석해 적절한 카테고리를 탐색합니다.\n\n"
                f"지원 의도: {intent_list}\n\n"
                "예시:\n"
                "  '청년정책 보고서 작성 중' → 청년 고용·주거·교육 통계표\n"
                "  '저소득 한부모 가정 지원 정책 마련' → 한부모·저소득·복지 통계표\n"
                "  '인구 소멸 논문' → 출산율·고령화·인구이동 통계표\n"
                "  '지역별 고령화 현황 분석' → 고령화·지역·노인 통계표"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "연구/정책 의도 설명 (자연어, 한국어)"
                    },
                    "max_results": {
                        "type": "integer",
                        "default": 12,
                        "description": "최대 반환 통계표 수"
                    },
                },
                "required": ["query"],
            },
        ),

        # ── 2. 데이터 조회 + 차트 통합 (NEW) ────────────────────────────────
        types.Tool(
            name="kosis_analyze",
            description=(
                "KOSIS 통계표 데이터를 조회하고 차트를 한 번에 생성합니다.\n"
                "데이터 조회 후 별도로 차트 툴을 호출할 필요 없이 이 툴 하나로 완료됩니다.\n\n"
                "chart_type:\n"
                "  line       – 시계열 추이 (연도별 변화)\n"
                "  multi_line – 복수 분류 비교 (지역별·성별 등)\n"
                "  bar        – 세로 막대\n"
                "  bar_h      – 가로 막대 (지역·순위 비교)\n"
                "  area       – 면적형\n"
                "  pie        – 구성비 (최신 시점)\n"
                "  scatter    – 산점도"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "org_id": {"type": "string", "description": "기관코드 (예: '101')"},
                    "tbl_id": {"type": "string", "description": "통계표 ID (예: 'DT_1IN1502')"},
                    "title": {"type": "string", "description": "차트 제목"},
                    "chart_type": {
                        "type": "string",
                        "enum": list(CHART_TYPES),
                        "default": "line"
                    },
                    "start_year": {"type": "string", "description": "시작 연도 (예: '2010')"},
                    "end_year": {"type": "string", "description": "종료 연도 (예: '2024')"},
                    "recent_n": {
                        "type": "integer",
                        "default": 20,
                        "description": "최근 N개 시점 (start_year 미지정 시)"
                    },
                    "prd_se": {
                        "type": "string",
                        "default": "Y",
                        "description": "주기: Y=연, M=월, Q=분기"
                    },
                    "color_field": {
                        "type": "string",
                        "description": "색상 구분 컬럼 (예: 'C1_NM'=지역, 'ITM_NM'=항목)"
                    },
                },
                "required": ["org_id", "tbl_id", "title"],
            },
        ),

        # ── 3. 카테고리 탐색 ─────────────────────────────────────────────────
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

        # ── 4. 통계표 설명 조회 ──────────────────────────────────────────────
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

        # ── 5. 다중 차트 대시보드 ────────────────────────────────────────────
        types.Tool(
            name="kosis_dashboard",
            description=(
                "여러 통계표를 한 화면의 대시보드로 시각화합니다.\n"
                "각 dataset은 kosis_analyze 대신 직접 데이터를 제공할 때 사용합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "datasets": {
                        "type": "array",
                        "description": "각 차트 설정 목록",
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


@mcp_app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent | types.ImageContent]:
    client = _get_client()

    # ── kosis_find_by_intent ─────────────────────────────────────────────────
    if name == "kosis_find_by_intent":
        result = await client.search_by_intent(
            query=arguments["query"],
            max_results=arguments.get("max_results", 12),
        )
        return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    # ── kosis_analyze (핵심: 조회 + 차트 한번에) ────────────────────────────
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

        # 데이터 조회
        data = await client.get_statistics_data(
            org_id=org_id,
            tbl_id=tbl_id,
            prd_se=prd_se,
            start_prd_de=start_year,
            end_prd_de=end_year,
            new_est_prd_cnt=recent_n,
        )

        if not data:
            return [types.TextContent(type="text", text="데이터가 없습니다.")]

        # color_field 자동 감지 (multi_line인데 color_field 미지정 시)
        if chart_type == "multi_line" and not color_field:
            sample_keys = list(data[0].keys()) if data else []
            for c in ("ITM_NM", "C1_NM", "C2_NM"):
                if c in sample_keys:
                    color_field = c
                    break

        # 차트 생성
        chart = create_chart(
            data=data,
            chart_type=chart_type,
            title=title,
            color_field=color_field,
            output_dir=OUTPUT_DIR,
            file_stem=title.replace(" ", "_")[:40],
        )

        # 요약 텍스트 (간결하게 — 전체 데이터 덤프 없음)
        summary = chart.get("summary", {})
        unit = ""
        if data and "UNIT_NM" in data[0]:
            unit = data[0]["UNIT_NM"] or ""

        info = {
            "title": title,
            "org_id": org_id,
            "tbl_id": tbl_id,
            "rows": len(data),
            "unit": unit,
            "summary": summary,
            "chart_type": chart_type,
        }
        if chart.get("html_path"):
            info["html_saved"] = chart["html_path"]

        contents: list = [
            types.TextContent(type="text", text=json.dumps(info, ensure_ascii=False, indent=2))
        ]

        # 이미지 첨부 (PNG 우선, 실패 시 HTML base64)
        if chart.get("base64_png"):
            contents.append(
                types.ImageContent(type="image", data=chart["base64_png"], mimeType="image/png")
            )
        elif chart.get("html_b64"):
            # HTML을 SVG처럼 반환 (Claude가 표시 가능)
            contents.append(
                types.TextContent(
                    type="text",
                    text=f"[차트 HTML 생성됨 — PNG 렌더링 불가. html_saved 경로 참고]"
                )
            )

        return contents

    # ── kosis_browse ────────────────────────────────────────────────────────
    elif name == "kosis_browse":
        result = await client.browse_categories(
            vw_cd=arguments.get("vw_cd", "MT_ZTITLE"),
            parent_list_id=arguments.get("parent_list_id", "A"),
        )
        tables = [
            {"org_id": r.get("ORG_ID"), "tbl_id": r.get("TBL_ID"), "name": r.get("TBL_NM"), "updated": r.get("SEND_DE")}
            for r in result if r.get("TBL_ID")
        ]
        cats = [
            {"list_id": r.get("LIST_ID"), "name": r.get("LIST_NM")}
            for r in result if r.get("LIST_ID") and not r.get("TBL_ID")
        ]
        out = {
            "sub_categories": cats,
            "tables": tables[:30],
            "tip": "sub_categories의 list_id를 parent_list_id에 넣어 더 탐색하거나, tables의 org_id+tbl_id로 kosis_analyze 호출"
        }
        return [types.TextContent(type="text", text=json.dumps(out, ensure_ascii=False, indent=2))]

    # ── kosis_explain ───────────────────────────────────────────────────────
    elif name == "kosis_explain":
        data = await client.get_statistics_explanation(
            org_id=arguments["org_id"],
            tbl_id=arguments["tbl_id"],
        )
        # 핵심 필드만 추출
        key_fields = {"TBL_NM", "STAT_NM", "CYCLE", "SURVEY_PURPOSE", "SURVEY_RANGE", "CONTACT_ORG"}
        compact = []
        for row in data:
            item = {k: v for k, v in row.items() if k in key_fields or not k.endswith("_CD")}
            compact.append(item)
        return [types.TextContent(type="text", text=json.dumps(compact[:5], ensure_ascii=False, indent=2))]

    # ── kosis_dashboard ─────────────────────────────────────────────────────
    elif name == "kosis_dashboard":
        datasets_cfg = arguments["datasets"]

        # 각 데이터셋 병렬 조회
        async def fetch_ds(ds_cfg: dict) -> dict:
            data = await client.get_statistics_data(
                org_id=ds_cfg["org_id"],
                tbl_id=ds_cfg["tbl_id"],
                prd_se=ds_cfg.get("prd_se", "Y"),
                start_prd_de=ds_cfg.get("start_year"),
                end_prd_de=ds_cfg.get("end_year"),
                new_est_prd_cnt=20,
            )
            return {
                "data": data,
                "title": ds_cfg["title"],
                "chart_type": ds_cfg.get("chart_type", "line"),
                "color_field": ds_cfg.get("color_field"),
            }

        fetched = await asyncio.gather(*[fetch_ds(ds) for ds in datasets_cfg], return_exceptions=True)
        valid_datasets = [f for f in fetched if isinstance(f, dict) and f.get("data")]

        if not valid_datasets:
            return [types.TextContent(type="text", text="조회된 데이터가 없습니다.")]

        result = create_dashboard(
            datasets=valid_datasets,
            output_dir=OUTPUT_DIR,
            file_stem=arguments.get("dashboard_title", "dashboard").replace(" ", "_")[:40],
        )

        contents: list = [
            types.TextContent(type="text", text=json.dumps({
                "dashboard_title": arguments.get("dashboard_title", ""),
                "charts": len(valid_datasets),
                "html_path": result.get("html_path"),
            }, ensure_ascii=False, indent=2))
        ]
        if result.get("base64_png"):
            contents.append(
                types.ImageContent(type="image", data=result["base64_png"], mimeType="image/png")
            )
        return contents

    raise ValueError(f"알 수 없는 도구: {name}")


# ── SSE transport 및 Starlette 앱 ─────────────────────────────────────────────
sse_transport = SseServerTransport("/messages/")


async def handle_sse(request: Request):
    api_key = request.query_params.get("kosis_key", "") or DEFAULT_API_KEY
    token = _api_key_ctx.set(api_key)
    try:
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())
    finally:
        _api_key_ctx.reset(token)


async def handle_health(request: Request):
    return JSONResponse({"status": "ok", "server": "kosis-mcp"})


async def handle_index(request: Request):
    host = request.headers.get("host", "localhost")
    scheme = "https" if request.headers.get("x-forwarded-proto") == "https" else "http"
    base_url = f"{scheme}://{host}"

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KOSIS MCP 서버</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 820px; margin: 60px auto; padding: 0 24px; color: #1a1a1a; }}
  h1 {{ font-size: 1.8rem; }}
  .badge {{ background: #22c55e; color: white; font-size: 0.75rem;
            padding: 2px 10px; border-radius: 99px; margin-left: 8px; }}
  .box {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px;
          padding: 24px; margin: 20px 0; }}
  .box h2 {{ margin-top: 0; font-size: 0.9rem; color: #475569; text-transform: uppercase; letter-spacing:.05em; }}
  code {{ background: #1e293b; color: #7dd3fc; padding: 12px 16px;
          border-radius: 8px; display: block; font-size: 0.85rem; white-space: pre-wrap; }}
  input {{ width:100%; box-sizing:border-box; padding:10px 14px;
           border:1px solid #cbd5e1; border-radius:8px; font-size:.95rem; }}
  button {{ margin-top:10px; padding:10px 22px; background:#3b82f6;
            color:white; border:none; border-radius:8px; cursor:pointer; }}
  button:hover {{ background:#2563eb; }}
  #out {{ margin-top:14px; }}
  li {{ margin-bottom: 6px; line-height: 1.7; }}
</style>
</head>
<body>
<h1>📊 KOSIS MCP 서버 <span class="badge">Running</span></h1>
<p>KOSIS 국가통계포털 데이터를 AI가 검색·분석·시각화하는 MCP 서버입니다.</p>

<div class="box">
  <h2>🔑 Step 1 — KOSIS 인증키 입력</h2>
  <input id="key" type="text" placeholder="KOSIS OpenAPI 인증키 (kosis.kr/openapi 에서 발급)" />
  <button onclick="gen()">접속 URL 생성</button>
  <div id="out"></div>
</div>

<div class="box">
  <h2>⚙️ Step 2 — Claude 연결</h2>
  <p>생성된 URL을 Claude Desktop의 커스텀 MCP 커넥터로 등록하거나 <code id="cfg">URL 생성 후 여기에 설정이 표시됩니다.</code></p>
</div>

<div class="box">
  <h2>💬 사용 예시 (의도 기반 검색)</h2>
  <ul>
    <li>"청년정책 보고서 작성 중인데 관련 통계 찾아줘"</li>
    <li>"저소득 한부모 가정을 위한 정책 마련 중이야, 관련 KOSIS 통계 분석해줘"</li>
    <li>"인구 소멸에 관한 논문 쓰고 있어. 출산율·고령화 차트 만들어줘"</li>
    <li>"지역별 고령화율 비교 차트 보여줘"</li>
  </ul>
</div>

<script>
function gen() {{
  const k = document.getElementById('key').value.trim();
  if (!k) {{ alert('인증키를 입력하세요'); return; }}
  const url = `{base_url}/sse?kosis_key=${{k}}`;
  document.getElementById('out').innerHTML = '<p style="margin:10px 0 4px;font-weight:600">접속 URL:</p><code>' + url + '</code>';
  document.getElementById('cfg').textContent = JSON.stringify({{"mcpServers":{{"kosis":{{"url": url}}}}}}, null, 2);
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)


starlette_app = Starlette(
    routes=[
        Route("/", endpoint=handle_index),
        Route("/health", endpoint=handle_health),
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse_transport.handle_post_message),
    ]
)

starlette_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"🚀 KOSIS MCP 서버 → http://0.0.0.0:{port}")
    uvicorn.run(starlette_app, host="0.0.0.0", port=port)
