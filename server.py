"""
KOSIS MCP 서버 (경량화판) — SSE / HTTP transport

변경 사항:
  - 차트 생성 제거: 데이터 + chart_hint 반환 → Claude 자체 시각화 도구 활용
  - errMsg=Y 파라미터 추가 (KOSIS API 필수값)
  - plotly/kaleido 의존성 완전 제거

접속 URL:
    https://your-server.com/sse?kosis_key=발급받은_인증키
"""

import asyncio
import contextvars
import json
import os
from pathlib import Path

import pandas as pd
import uvicorn
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp import types
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Mount, Route

from kosis_client import KosisClient, INTENT_MAP

# ── 설정 ──────────────────────────────────────────────────────────────────────
DEFAULT_API_KEY = os.environ.get("KOSIS_API_KEY", "")

_api_key_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "kosis_api_key", default=DEFAULT_API_KEY
)


def _get_client() -> KosisClient:
    key = _api_key_ctx.get()
    if not key:
        raise ValueError("KOSIS 인증키 없음. URL에 ?kosis_key=YOUR_KEY 추가 필요.")
    return KosisClient(key)


# ── 데이터 전처리 헬퍼 ────────────────────────────────────────────────────────
_KEEP_FIELDS = {"PRD_DE", "DT", "ITM_NM", "C1_NM", "C2_NM", "C3_NM", "UNIT_NM"}


def _process_data(data: list[dict], color_field: str | None = None) -> tuple[list[dict], dict, str]:
    """
    데이터 정제 + 요약 통계 계산.
    Returns: (filtered_rows, summary, unit)
    """
    if not data:
        return [], {}, ""

    unit = data[0].get("UNIT_NM", "") or ""

    # 필요 컬럼만 추출
    rows = [{k: v for k, v in row.items() if k in _KEEP_FIELDS} for row in data]

    df = pd.DataFrame(rows)
    if "DT" in df.columns:
        df["DT"] = pd.to_numeric(
            df["DT"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

    # color_field 카테고리 과다 시 필터링 (>12 → 전국/합계 or 상위 10)
    if color_field and color_field in df.columns and df[color_field].nunique() > 12:
        mask = df[color_field].astype(str).str.contains(
            "전국|합계|전체|계$", na=False, regex=True
        )
        if mask.any():
            df = df[mask].copy()
        elif "DT" in df.columns:
            top_vals = (
                df.groupby(color_field)["DT"].mean().dropna().nlargest(10).index
            )
            df = df[df[color_field].isin(top_vals)].copy()

    # 요약 통계
    summary: dict = {}
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


# ── MCP 서버 ──────────────────────────────────────────────────────────────────
mcp_app = Server("kosis-mcp")


@mcp_app.list_tools()
async def list_tools() -> list[types.Tool]:
    intent_list = ", ".join(list(INTENT_MAP.keys())[:10]) + " 등"
    return [
        # ── 1. 의도 기반 통계 탐색 ───────────────────────────────────────────
        types.Tool(
            name="kosis_find_by_intent",
            description=(
                "사용자의 연구/정책 의도를 자연어로 입력하면 관련 KOSIS 통계표를 자동으로 찾아줍니다.\n"
                "단순 키워드 검색과 달리 의도를 분석해 적절한 카테고리를 탐색합니다.\n\n"
                f"지원 의도: {intent_list}\n\n"
                "예시:\n"
                "  '청년정책 보고서 작성 중' → 청년 고용·주거·교육 통계표\n"
                "  '저소득 한부모 가정 지원 정책 마련' → 한부모·저소득·복지 통계표\n"
                "  '인구 소멸 논문' → 출산율·고령화·인구이동 통계표"
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

        # ── 2. 데이터 조회 (차트 힌트 포함) ─────────────────────────────────
        types.Tool(
            name="kosis_analyze",
            description=(
                "KOSIS 통계표 데이터를 조회하고 chart_hint와 함께 반환합니다.\n"
                "반환된 data 배열과 chart_hint를 활용해 Claude가 직접 시각화를 생성합니다.\n\n"
                "chart_type 값:\n"
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
                        "enum": ["line", "bar", "bar_h", "pie", "scatter", "area", "multi_line"],
                        "default": "line",
                        "description": "권장 차트 유형"
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

        # ── 5. 다중 통계 대시보드 ────────────────────────────────────────────
        types.Tool(
            name="kosis_dashboard",
            description=(
                "여러 통계표 데이터를 한꺼번에 조회해 반환합니다.\n"
                "반환된 datasets 배열을 활용해 Claude가 대시보드 형태로 시각화를 생성합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "datasets": {
                        "type": "array",
                        "description": "조회할 통계표 목록",
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
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    client = _get_client()

    # ── kosis_find_by_intent ─────────────────────────────────────────────────
    if name == "kosis_find_by_intent":
        result = await client.search_by_intent(
            query=arguments["query"],
            max_results=arguments.get("max_results", 12),
        )
        return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    # ── kosis_analyze ────────────────────────────────────────────────────────
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
            org_id=org_id,
            tbl_id=tbl_id,
            prd_se=prd_se,
            start_prd_de=start_year,
            end_prd_de=end_year,
            new_est_prd_cnt=recent_n,
        )

        if not data:
            return [types.TextContent(type="text", text="데이터가 없습니다.")]

        # color_field 자동 감지
        if not color_field:
            sample_keys = set(data[0].keys()) if data else set()
            for c in ("ITM_NM", "C1_NM", "C2_NM"):
                if c in sample_keys:
                    color_field = c
                    break

        rows, summary, unit = _process_data(data, color_field)

        result = {
            "title": title,
            "org_id": org_id,
            "tbl_id": tbl_id,
            "unit": unit,
            "rows": len(rows),
            "summary": summary,
            "chart_hint": {
                "chart_type": chart_type,
                "x_field": "PRD_DE",
                "y_field": "DT",
                "color_field": color_field,
            },
            "data": rows[:300],  # 최대 300행
        }

        return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    # ── kosis_browse ────────────────────────────────────────────────────────
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
        out = {
            "sub_categories": cats,
            "tables": tables[:30],
            "tip": "sub_categories의 list_id를 parent_list_id에 넣어 더 탐색하거나, tables의 org_id+tbl_id로 kosis_analyze 호출",
        }
        return [types.TextContent(type="text", text=json.dumps(out, ensure_ascii=False, indent=2))]

    # ── kosis_explain ───────────────────────────────────────────────────────
    elif name == "kosis_explain":
        data = await client.get_statistics_explanation(
            org_id=arguments["org_id"],
            tbl_id=arguments["tbl_id"],
        )
        key_fields = {"TBL_NM", "STAT_NM", "CYCLE", "SURVEY_PURPOSE", "SURVEY_RANGE", "CONTACT_ORG"}
        compact = [
            {k: v for k, v in row.items() if k in key_fields or not k.endswith("_CD")}
            for row in data
        ]
        return [types.TextContent(type="text", text=json.dumps(compact[:5], ensure_ascii=False, indent=2))]

    # ── kosis_dashboard ─────────────────────────────────────────────────────
    elif name == "kosis_dashboard":
        datasets_cfg = arguments["datasets"]

        async def fetch_ds(ds_cfg: dict) -> dict | None:
            try:
                data = await client.get_statistics_data(
                    org_id=ds_cfg["org_id"],
                    tbl_id=ds_cfg["tbl_id"],
                    prd_se=ds_cfg.get("prd_se", "Y"),
                    start_prd_de=ds_cfg.get("start_year"),
                    end_prd_de=ds_cfg.get("end_year"),
                    new_est_prd_cnt=20,
                )
                if not data:
                    return None
                color_field = ds_cfg.get("color_field")
                if not color_field:
                    sample_keys = set(data[0].keys())
                    for c in ("ITM_NM", "C1_NM", "C2_NM"):
                        if c in sample_keys:
                            color_field = c
                            break
                rows, summary, unit = _process_data(data, color_field)
                return {
                    "title": ds_cfg["title"],
                    "org_id": ds_cfg["org_id"],
                    "tbl_id": ds_cfg["tbl_id"],
                    "unit": unit,
                    "rows": len(rows),
                    "summary": summary,
                    "chart_hint": {
                        "chart_type": ds_cfg.get("chart_type", "line"),
                        "x_field": "PRD_DE",
                        "y_field": "DT",
                        "color_field": color_field,
                    },
                    "data": rows[:150],
                }
            except Exception as e:
                return {"title": ds_cfg.get("title", ""), "error": str(e)}

        fetched = await asyncio.gather(*[fetch_ds(ds) for ds in datasets_cfg])
        datasets_out = [f for f in fetched if f is not None]

        result = {
            "dashboard_title": arguments.get("dashboard_title", "KOSIS 통계 대시보드"),
            "count": len(datasets_out),
            "datasets": datasets_out,
        }
        return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    raise ValueError(f"알 수 없는 도구: {name}")


# ── SSE transport 및 Starlette 앱 ─────────────────────────────────────────────
sse_transport = SseServerTransport("/messages/")


async def handle_sse(request: Request) -> Response:
    api_key = request.query_params.get("kosis_key", "") or DEFAULT_API_KEY
    token = _api_key_ctx.set(api_key)
    try:
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())
    finally:
        _api_key_ctx.reset(token)
    return Response()  # Starlette 1.0.0: request_response 래퍼가 반환값을 Response로 호출함


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
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
       background:#f1f5f9;color:#1a1a1a;}}
  .hero{{background:linear-gradient(135deg,#1e3a5f 0%,#2563eb 100%);
         color:white;padding:56px 24px 48px;text-align:center;}}
  .hero h1{{font-size:2rem;font-weight:700;margin-bottom:10px;}}
  .hero p{{font-size:1.05rem;opacity:.85;max-width:600px;margin:0 auto;}}
  .badge{{background:#22c55e;color:white;font-size:.72rem;padding:3px 11px;
          border-radius:99px;vertical-align:middle;margin-left:8px;}}
  .wrap{{max-width:860px;margin:0 auto;padding:32px 20px 60px;}}
  .card{{background:white;border:1px solid #e2e8f0;border-radius:14px;
         padding:28px;margin-bottom:24px;}}
  .card h2{{font-size:1rem;font-weight:700;margin-bottom:16px;
            display:flex;align-items:center;gap:8px;}}
  .step-num{{background:#2563eb;color:white;width:24px;height:24px;
             border-radius:50%;display:inline-flex;align-items:center;
             justify-content:center;font-size:.8rem;font-weight:700;flex-shrink:0;}}
  code{{background:#0f172a;color:#7dd3fc;padding:13px 16px;border-radius:9px;
        display:block;font-size:.84rem;white-space:pre-wrap;word-break:break-all;line-height:1.6;}}
  .inline-code{{background:#f1f5f9;color:#0f172a;padding:2px 7px;border-radius:4px;
                font-size:.88em;font-family:monospace;}}
  input{{width:100%;padding:11px 14px;border:1px solid #cbd5e1;border-radius:9px;
         font-size:.95rem;margin-bottom:2px;}}
  input:focus{{outline:none;border-color:#2563eb;box-shadow:0 0 0 3px rgba(37,99,235,.15);}}
  .btn{{margin-top:10px;padding:11px 26px;background:#2563eb;color:white;
        border:none;border-radius:9px;cursor:pointer;font-size:.95rem;font-weight:600;}}
  .btn:hover{{background:#1d4ed8;}}
  #url-out{{margin-top:14px;}}
  .tool-grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px;}}
  @media(max-width:600px){{.tool-grid{{grid-template-columns:1fr;}}}}
  .tool-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;}}
  .tool-card .name{{font-family:monospace;font-size:.82rem;color:#2563eb;
                    font-weight:700;margin-bottom:6px;}}
  .tool-card p{{font-size:.88rem;color:#475569;line-height:1.5;}}
  .example-list{{list-style:none;}}
  .example-list li{{padding:11px 14px;background:#f8fafc;border:1px solid #e2e8f0;
                    border-radius:9px;margin-bottom:9px;font-size:.9rem;line-height:1.5;
                    cursor:pointer;transition:background .15s;}}
  .example-list li:hover{{background:#eff6ff;border-color:#bfdbfe;}}
  .example-list li::before{{content:"💬 ";}}
  .intent-wrap{{display:flex;flex-wrap:wrap;gap:8px;margin-top:4px;}}
  .intent-tag{{background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;
               padding:4px 12px;border-radius:99px;font-size:.82rem;}}
  .divider{{border:none;border-top:1px solid #e2e8f0;margin:8px 0 16px;}}
  a{{color:#2563eb;text-decoration:none;}}
  a:hover{{text-decoration:underline;}}
  .footer{{text-align:center;padding:24px;font-size:.83rem;color:#94a3b8;}}
</style>
</head>
<body>

<div class="hero">
  <h1>📊 KOSIS MCP 서버 <span class="badge">● Running</span></h1>
  <p>KOSIS 국가통계포털 데이터를 Claude AI가 자동으로 검색·분석·시각화해주는 MCP 서버</p>
</div>

<div class="wrap">

  <div class="card">
    <h2><span class="step-num">1</span> KOSIS 인증키 발급</h2>
    <p style="margin-bottom:14px;font-size:.92rem;color:#475569;">
      <a href="https://kosis.kr/openapi/" target="_blank">kosis.kr/openapi</a>에서 회원가입 후
      <strong>활용신청 → 인증키 발급</strong>을 받으세요. 즉시 발급됩니다.
    </p>
    <input id="api-key" type="text" placeholder="발급받은 KOSIS 인증키를 입력하세요" />
    <button class="btn" onclick="generateUrl()">접속 URL 생성 →</button>
    <div id="url-out"></div>
  </div>

  <div class="card">
    <h2><span class="step-num">2</span> Claude에 연결</h2>
    <p style="font-size:.92rem;color:#475569;margin-bottom:14px;">
      Claude 앱 → <strong>Settings → Integrations → Add custom integration</strong>에
      생성된 URL을 붙여넣으세요.
    </p>
    <code id="config-box">{base_url}/sse?kosis_key=YOUR_KEY</code>
  </div>

  <div class="card">
    <h2><span class="step-num">3</span> 이렇게 말해보세요</h2>
    <ul class="example-list" id="examples">
      <li data-text="청년정책 보고서 작성 중인데, 청년 고용·주거·교육 관련 KOSIS 통계 찾아서 분석해줘">청년정책 보고서 작성 중인데, 청년 고용·주거·교육 관련 KOSIS 통계 찾아서 분석해줘</li>
      <li data-text="저소득 한부모 가정을 위한 정책 마련 중이야. 관련 통계 찾아서 차트로 보여줘">저소득 한부모 가정을 위한 정책 마련 중이야. 관련 통계 찾아서 차트로 보여줘</li>
      <li data-text="인구 소멸에 관한 논문 쓰고 있어. 합계출산율과 고령화율 추이 그래프 만들어줘">인구 소멸에 관한 논문 쓰고 있어. 합계출산율과 고령화율 추이 그래프 만들어줘</li>
      <li data-text="최근 10년간 청년 실업률 추이를 꺾은선 그래프로 보여줘">최근 10년간 청년 실업률 추이를 꺾은선 그래프로 보여줘</li>
      <li data-text="지역별 고령화율 현황을 비교 차트로 만들어줘">지역별 고령화율 현황을 비교 차트로 만들어줘</li>
    </ul>
  </div>

  <div class="card">
    <h2>🗂 지원하는 연구·정책 분야</h2>
    <hr class="divider">
    <div class="intent-wrap">
      <span class="intent-tag">청년</span><span class="intent-tag">아동·보육</span>
      <span class="intent-tag">청소년</span><span class="intent-tag">노인·고령자</span>
      <span class="intent-tag">여성</span><span class="intent-tag">장애인</span>
      <span class="intent-tag">다문화</span><span class="intent-tag">한부모</span>
      <span class="intent-tag">저출산</span><span class="intent-tag">고령화</span>
      <span class="intent-tag">인구소멸</span><span class="intent-tag">1인가구</span>
      <span class="intent-tag">저소득·빈곤</span><span class="intent-tag">고용·실업</span>
      <span class="intent-tag">교육</span><span class="intent-tag">주거·주택</span>
      <span class="intent-tag">소득·임금</span><span class="intent-tag">복지</span>
      <span class="intent-tag">보건·의료</span><span class="intent-tag">인구</span>
      <span class="intent-tag">지역균형</span>
    </div>
  </div>

  <div class="card">
    <h2>⚙️ 제공 기능 (MCP 도구)</h2>
    <hr class="divider">
    <div class="tool-grid">
      <div class="tool-card">
        <div class="name">kosis_find_by_intent</div>
        <p>자연어로 연구 의도를 설명하면 관련 KOSIS 통계표를 자동 탐색합니다.</p>
      </div>
      <div class="tool-card">
        <div class="name">kosis_analyze</div>
        <p>통계표 데이터를 조회해 반환합니다. Claude가 데이터를 받아 차트를 직접 생성합니다.</p>
      </div>
      <div class="tool-card">
        <div class="name">kosis_dashboard</div>
        <p>여러 통계표 데이터를 한꺼번에 조회합니다. Claude가 대시보드로 시각화합니다.</p>
      </div>
      <div class="tool-card">
        <div class="name">kosis_browse</div>
        <p>KOSIS 카테고리 트리를 직접 탐색해 원하는 통계표를 찾습니다.</p>
      </div>
    </div>
  </div>

</div>

<div class="footer">
  KOSIS MCP Server · Built with <a href="https://modelcontextprotocol.io" target="_blank">MCP</a> ·
  데이터 출처: <a href="https://kosis.kr" target="_blank">통계청 KOSIS</a>
</div>

<script>
function generateUrl() {{
  const key = document.getElementById('api-key').value.trim();
  if (!key) {{ alert('인증키를 입력해주세요'); return; }}
  const url = `{base_url}/sse?kosis_key=${{key}}`;
  document.getElementById('url-out').innerHTML =
    '<p style="margin:12px 0 6px;font-weight:600;font-size:.9rem;">접속 URL:</p>' +
    '<code style="background:#0f172a;color:#7dd3fc;padding:13px 16px;border-radius:9px;display:block;font-size:.84rem;word-break:break-all;">' + url + '</code>';
  document.getElementById('config-box').textContent = url;
}}
document.getElementById('examples').addEventListener('click', function(e) {{
  const li = e.target.closest('li');
  if (!li) return;
  navigator.clipboard.writeText(li.dataset.text).then(() => {{
    const orig = li.style.background;
    li.style.background = '#dcfce7';
    setTimeout(() => li.style.background = orig, 600);
  }});
}});
</script>
</body>
</html>"""
    return HTMLResponse(html)


class _SseMsgApp:
    """handle_post_message를 라우트 정의 시점이 아닌 요청 시점에 바인딩.
    MCP SDK 버전에 따라 handle_post_message가 초기화 전에 None일 수 있어서 래핑."""
    async def __call__(self, scope, receive, send):
        await sse_transport.handle_post_message(scope, receive, send)


starlette_app = Starlette(
    routes=[
        Route("/", endpoint=handle_index),
        Route("/health", endpoint=handle_health),
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=_SseMsgApp()),
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
