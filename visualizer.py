"""
KOSIS 통계 데이터 시각화 모듈.

plotly를 사용해 인터랙티브 차트(HTML)와 정적 이미지(PNG base64)를 생성합니다.
pandas DataFrame으로 데이터를 정제한 뒤 plotly Express / Graph Objects로 렌더링.
"""

import base64
import io
import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# ──────────────────────────────────────────────────────────────────────────────
# 내부 헬퍼
# ──────────────────────────────────────────────────────────────────────────────

def _to_df(data: list[dict]) -> pd.DataFrame:
    """KOSIS API 응답 리스트를 DataFrame으로 변환하고 기본 타입을 정제합니다."""
    df = pd.DataFrame(data)
    if df.empty:
        return df

    # 수치값 DT: 쉼표 제거 후 숫자 변환
    if "DT" in df.columns:
        df["DT"] = (
            df["DT"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df["DT"] = pd.to_numeric(df["DT"], errors="coerce")

    # 시점(PRD_DE): 연도 4자리는 int, 그 외 str 유지
    if "PRD_DE" in df.columns:
        df["PRD_DE"] = df["PRD_DE"].astype(str)

    return df


def _fig_to_base64(fig) -> str:
    """plotly Figure → PNG base64 문자열."""
    try:
        img_bytes = fig.to_image(format="png", width=900, height=500, scale=1.5)
        return base64.b64encode(img_bytes).decode("utf-8")
    except Exception:
        # kaleido 미설치 시 SVG fallback
        svg_str = fig.to_image(format="svg")
        return base64.b64encode(svg_str).decode("utf-8")


def _save_html(fig, path: str):
    """plotly Figure를 인터랙티브 HTML로 저장."""
    fig.write_html(path, include_plotlyjs="cdn")


# ──────────────────────────────────────────────────────────────────────────────
# 공개 API
# ──────────────────────────────────────────────────────────────────────────────

CHART_TYPES = ("line", "bar", "bar_h", "pie", "scatter", "area", "multi_line")


def create_chart(
    data: list[dict] | str,
    chart_type: str = "line",
    title: str = "통계 차트",
    x_field: str = "PRD_DE",
    y_field: str = "DT",
    color_field: Optional[str] = None,
    output_dir: Optional[str] = None,
    file_stem: Optional[str] = None,
) -> dict:
    """
    KOSIS 통계 데이터를 시각화합니다.

    Args:
        data: KOSIS API 응답 리스트 또는 JSON 문자열
        chart_type: 차트 유형
            'line'       - 꺾은선 (시계열 추이, 기본값)
            'bar'        - 세로 막대
            'bar_h'      - 가로 막대 (지역별 비교 등)
            'pie'        - 원형 (구성비)
            'scatter'    - 산점도
            'area'       - 면적형 (누적 추이)
            'multi_line' - 다중 꺾은선 (복수 분류 비교)
        title: 차트 제목
        x_field: X축 컬럼명 (기본 'PRD_DE' = 시점)
        y_field: Y축 컬럼명 (기본 'DT' = 수치값)
        color_field: 색상 구분 컬럼명 (예: 'C1_NM', 'ITM_NM')
        output_dir: 파일 저장 디렉터리 (None이면 저장 안 함)
        file_stem: 파일명 기본값 (확장자 제외)

    Returns:
        {
            "base64_png": str,   # PNG 이미지 base64
            "html_path":  str,   # 저장된 HTML 경로 (output_dir 지정 시)
            "png_path":   str,   # 저장된 PNG 경로 (output_dir 지정 시)
            "summary":    dict,  # 데이터 요약 통계
            "columns":    list,  # DataFrame 컬럼 목록
        }
    """
    # ── 데이터 파싱 ──────────────────────────────
    if isinstance(data, str):
        data = json.loads(data)

    df = _to_df(data)
    if df.empty:
        raise ValueError("시각화할 데이터가 없습니다.")

    # 누락 컬럼 보정
    if x_field not in df.columns:
        x_field = df.columns[0]
    if y_field not in df.columns:
        y_field = df.columns[1] if len(df.columns) > 1 else df.columns[0]

    # ── 단위 레이블 추출 ────────────────────────
    unit = ""
    if "UNIT_NM" in df.columns:
        unit = df["UNIT_NM"].dropna().iloc[0] if not df["UNIT_NM"].dropna().empty else ""
    y_label = f"{y_field} ({unit})" if unit else y_field

    # ── plotly 차트 생성 ────────────────────────
    common_kwargs = dict(
        x=x_field,
        y=y_field,
        title=title,
        labels={y_field: y_label, x_field: "시점"},
        template="plotly_white",
    )
    if color_field and color_field in df.columns:
        common_kwargs["color"] = color_field

    if chart_type == "line":
        fig = px.line(df, **common_kwargs, markers=True)

    elif chart_type == "multi_line":
        # color_field 필수
        if not color_field or color_field not in df.columns:
            # ITM_NM 또는 C1_NM으로 자동 선택
            for candidate in ("ITM_NM", "C1_NM", "C2_NM"):
                if candidate in df.columns:
                    color_field = candidate
                    common_kwargs["color"] = color_field
                    break
        fig = px.line(df, **common_kwargs, markers=True)

    elif chart_type == "bar":
        fig = px.bar(df, **common_kwargs, barmode="group")

    elif chart_type == "bar_h":
        fig = px.bar(df, x=y_field, y=x_field, title=title,
                     labels={y_field: y_label},
                     template="plotly_white",
                     color=color_field if color_field and color_field in df.columns else None,
                     orientation="h")

    elif chart_type == "area":
        fig = px.area(df, **common_kwargs)

    elif chart_type == "pie":
        # 파이차트: 최신 시점 데이터만 사용
        latest = df[df[x_field] == df[x_field].max()] if pd.api.types.is_numeric_dtype(df[x_field]) else df
        names_col = color_field if color_field and color_field in df.columns else x_field
        fig = px.pie(
            latest,
            values=y_field,
            names=names_col,
            title=title,
            template="plotly_white",
        )

    elif chart_type == "scatter":
        fig = px.scatter(df, **common_kwargs, size_max=12)

    else:
        raise ValueError(f"지원하지 않는 chart_type: '{chart_type}'. 가능한 값: {CHART_TYPES}")

    # ── 레이아웃 개선 ───────────────────────────
    fig.update_layout(
        font=dict(family="Malgun Gothic, Arial", size=12),
        title_font_size=15,
        legend_title_font_size=11,
        margin=dict(l=60, r=30, t=60, b=60),
    )
    if chart_type in ("line", "multi_line", "bar", "area", "scatter"):
        fig.update_xaxes(tickangle=-30)

    # ── 출력 ────────────────────────────────────
    result: dict = {
        "base64_png": _fig_to_base64(fig),
        "html_path": None,
        "png_path": None,
        "summary": _summarize(df, y_field),
        "columns": list(df.columns),
    }

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        stem = file_stem or title.replace(" ", "_")[:40]

        html_path = str(out / f"{stem}.html")
        _save_html(fig, html_path)
        result["html_path"] = html_path

        try:
            png_path = str(out / f"{stem}.png")
            fig.write_image(png_path, width=900, height=500, scale=1.5)
            result["png_path"] = png_path
        except Exception:
            pass  # kaleido 없을 시 PNG 건너뜀

    return result


def create_dashboard(
    datasets: list[dict],
    output_dir: Optional[str] = None,
    file_stem: str = "kosis_dashboard",
) -> dict:
    """
    복수의 통계 데이터셋을 한 화면에 나란히 시각화합니다.
    논문 자료 정리 등 여러 지표를 한눈에 볼 때 유용합니다.

    Args:
        datasets: 각 원소는 create_chart 파라미터와 동일한 딕셔너리
            [
              {"data": [...], "chart_type": "line",  "title": "인구 추이", ...},
              {"data": [...], "chart_type": "bar",   "title": "지역별 출생아 수", ...},
            ]
        output_dir: 저장 디렉터리
        file_stem: 파일명 기본값

    Returns:
        {"html_path": str, "base64_png": str}
    """
    n = len(datasets)
    cols = min(n, 2)
    rows = (n + 1) // 2
    fig = make_subplots(rows=rows, cols=cols,
                        subplot_titles=[d.get("title", f"차트 {i+1}") for i, d in enumerate(datasets)])

    for i, ds in enumerate(datasets):
        row = i // cols + 1
        col = i % cols + 1
        sub_df = _to_df(ds["data"] if isinstance(ds["data"], list) else json.loads(ds["data"]))
        if sub_df.empty:
            continue
        x = ds.get("x_field", "PRD_DE")
        y = ds.get("y_field", "DT")
        if x not in sub_df.columns:
            x = sub_df.columns[0]
        if y not in sub_df.columns:
            y = sub_df.columns[1] if len(sub_df.columns) > 1 else sub_df.columns[0]

        ct = ds.get("chart_type", "line")
        if ct in ("line", "multi_line", "area"):
            trace = go.Scatter(x=sub_df[x], y=sub_df[y],
                               mode="lines+markers", name=ds.get("title", ""))
        else:
            trace = go.Bar(x=sub_df[x], y=sub_df[y], name=ds.get("title", ""))

        fig.add_trace(trace, row=row, col=col)

    fig.update_layout(
        template="plotly_white",
        font=dict(family="Malgun Gothic, Arial", size=11),
        showlegend=False,
        height=400 * rows,
    )

    result = {"html_path": None, "base64_png": _fig_to_base64(fig)}
    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        html_path = str(out / f"{file_stem}.html")
        _save_html(fig, html_path)
        result["html_path"] = html_path

        try:
            png_path = str(out / f"{file_stem}.png")
            fig.write_image(png_path, width=1200, height=400 * rows, scale=1.5)
            result["png_path"] = png_path
        except Exception:
            pass

    return result


def _summarize(df: pd.DataFrame, y_field: str) -> dict:
    """수치 컬럼에 대한 기초 통계량을 반환합니다."""
    if y_field not in df.columns or not pd.api.types.is_numeric_dtype(df[y_field]):
        return {}
    s = df[y_field].dropna()
    return {
        "count": int(s.count()),
        "min": float(s.min()),
        "max": float(s.max()),
        "mean": round(float(s.mean()), 2),
        "latest": float(s.iloc[-1]) if len(s) else None,
        "trend": "상승" if len(s) >= 2 and s.iloc[-1] > s.iloc[0] else "하락" if len(s) >= 2 else "N/A",
    }
