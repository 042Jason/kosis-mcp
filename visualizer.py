"""
KOSIS 통계 데이터 시각화 모듈 (개선판)
- kaleido 실패 시 HTML inline fallback
- 한국어 폰트 설정 강화
- 차트 품질 개선
"""

import base64
import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


CHART_TYPES = ("line", "bar", "bar_h", "pie", "scatter", "area", "multi_line")

# ──────────────────────────────────────────────────────────────────────────────
# 내부 헬퍼
# ──────────────────────────────────────────────────────────────────────────────

def _to_df(data: list[dict] | str) -> pd.DataFrame:
    if isinstance(data, str):
        data = json.loads(data)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    if "DT" in df.columns:
        df["DT"] = (
            df["DT"].astype(str).str.replace(",", "", regex=False).str.strip()
        )
        df["DT"] = pd.to_numeric(df["DT"], errors="coerce")
    if "PRD_DE" in df.columns:
        df["PRD_DE"] = df["PRD_DE"].astype(str)
    return df


def _fig_to_base64_png(fig) -> Optional[str]:
    """plotly Figure → PNG base64. 실패 시 None 반환."""
    try:
        img_bytes = fig.to_image(format="png", width=960, height=540, scale=1.5)
        return base64.b64encode(img_bytes).decode("utf-8")
    except Exception:
        return None


def _fig_to_html_b64(fig) -> str:
    """plotly Figure → 인터랙티브 HTML → base64 (PNG 실패 시 fallback)."""
    html_str = fig.to_html(include_plotlyjs="cdn", full_html=True)
    return base64.b64encode(html_str.encode("utf-8")).decode("utf-8")


def _save_html(fig, path: str):
    fig.write_html(path, include_plotlyjs="cdn")


def _apply_layout(fig, title: str):
    """공통 레이아웃: 한국어 폰트, 여백, 테마."""
    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        font=dict(family="Malgun Gothic, NanumGothic, Apple SD Gothic Neo, sans-serif", size=12),
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=30, t=70, b=60),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )


def _summarize(df: pd.DataFrame, y_field: str) -> dict:
    if y_field not in df.columns or not pd.api.types.is_numeric_dtype(df[y_field]):
        return {}
    s = df[y_field].dropna()
    if s.empty:
        return {}
    trend = "상승" if len(s) >= 2 and float(s.iloc[-1]) > float(s.iloc[0]) else (
        "하락" if len(s) >= 2 else "N/A"
    )
    change_pct = None
    if len(s) >= 2 and float(s.iloc[0]) != 0:
        change_pct = round((float(s.iloc[-1]) - float(s.iloc[0])) / abs(float(s.iloc[0])) * 100, 1)
    return {
        "count": int(s.count()),
        "min": round(float(s.min()), 3),
        "max": round(float(s.max()), 3),
        "mean": round(float(s.mean()), 3),
        "latest": round(float(s.iloc[-1]), 3),
        "trend": trend,
        "change_pct": change_pct,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 공개 API
# ──────────────────────────────────────────────────────────────────────────────

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

    Returns:
        {
            "base64_png":  str | None,  # PNG 이미지 base64 (kaleido 성공 시)
            "html_b64":    str,          # HTML base64 (항상 반환, PNG 실패 시 대체)
            "html_path":   str | None,
            "png_path":    str | None,
            "summary":     dict,
            "chart_type":  str,
        }
    """
    df = _to_df(data)
    if df.empty:
        raise ValueError("시각화할 데이터가 없습니다.")

    # 컬럼 보정
    if x_field not in df.columns:
        x_field = df.columns[0]
    if y_field not in df.columns:
        y_field = df.columns[1] if len(df.columns) > 1 else df.columns[0]

    # 단위 레이블
    unit = ""
    if "UNIT_NM" in df.columns:
        vals = df["UNIT_NM"].dropna()
        unit = vals.iloc[0] if not vals.empty else ""
    y_label = f"{y_field} ({unit})" if unit else y_field

    common = dict(
        x=x_field, y=y_field,
        labels={y_field: y_label, x_field: "시점"},
    )
    if color_field and color_field in df.columns:
        common["color"] = color_field

    # ── 차트 생성 ──────────────────────────────────────────
    if chart_type == "line":
        fig = px.line(df, **common, markers=True)

    elif chart_type == "multi_line":
        if not color_field or color_field not in df.columns:
            for c in ("ITM_NM", "C1_NM", "C2_NM"):
                if c in df.columns:
                    common["color"] = c
                    break
        fig = px.line(df, **common, markers=True)

    elif chart_type == "bar":
        fig = px.bar(df, **common, barmode="group")

    elif chart_type == "bar_h":
        cf = color_field if color_field and color_field in df.columns else None
        fig = px.bar(df, x=y_field, y=x_field,
                     labels={y_field: y_label},
                     color=cf, orientation="h")

    elif chart_type == "area":
        fig = px.area(df, **common)

    elif chart_type == "pie":
        latest_val = df[x_field].max() if pd.api.types.is_numeric_dtype(df[x_field]) else None
        subset = df[df[x_field] == latest_val] if latest_val else df
        names_col = color_field if color_field and color_field in df.columns else x_field
        fig = px.pie(subset, values=y_field, names=names_col)

    elif chart_type == "scatter":
        fig = px.scatter(df, **common, size_max=12)

    else:
        raise ValueError(f"지원하지 않는 chart_type: '{chart_type}'. 가능: {CHART_TYPES}")

    _apply_layout(fig, title)
    if chart_type not in ("pie",):
        fig.update_xaxes(tickangle=-30, showgrid=True, gridcolor="#f0f0f0")
        fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")

    # ── 출력 ──────────────────────────────────────────────
    base64_png = _fig_to_base64_png(fig)
    html_b64 = _fig_to_html_b64(fig)

    result: dict = {
        "base64_png": base64_png,
        "html_b64": html_b64,
        "html_path": None,
        "png_path": None,
        "summary": _summarize(df, y_field),
        "chart_type": chart_type,
        "rows_used": len(df),
    }

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        stem = file_stem or title.replace(" ", "_")[:40]

        html_path = str(out / f"{stem}.html")
        _save_html(fig, html_path)
        result["html_path"] = html_path

        if base64_png:
            try:
                png_path = str(out / f"{stem}.png")
                fig.write_image(png_path, width=960, height=540, scale=1.5)
                result["png_path"] = png_path
            except Exception:
                pass

    return result


def create_dashboard(
    datasets: list[dict],
    output_dir: Optional[str] = None,
    file_stem: str = "kosis_dashboard",
) -> dict:
    """복수 통계 데이터셋을 하나의 대시보드로 시각화."""
    n = len(datasets)
    cols = min(n, 2)
    rows = (n + 1) // 2
    fig = make_subplots(
        rows=rows, cols=cols,
        subplot_titles=[d.get("title", f"차트 {i+1}") for i, d in enumerate(datasets)],
        vertical_spacing=0.12,
    )

    for i, ds in enumerate(datasets):
        row = i // cols + 1
        col = i % cols + 1
        raw = ds.get("data") or ds.get("data_json", "[]")
        sub_df = _to_df(raw)
        if sub_df.empty:
            continue
        x = ds.get("x_field", "PRD_DE")
        y = ds.get("y_field", "DT")
        if x not in sub_df.columns:
            x = sub_df.columns[0]
        if y not in sub_df.columns:
            y = sub_df.columns[1] if len(sub_df.columns) > 1 else sub_df.columns[0]

        ct = ds.get("chart_type", "line")
        color = ds.get("color_field")
        name = ds.get("title", f"차트{i+1}")

        if ct in ("line", "multi_line", "area"):
            if color and color in sub_df.columns:
                for grp, gdf in sub_df.groupby(color):
                    fig.add_trace(go.Scatter(x=gdf[x], y=gdf[y],
                                             mode="lines+markers", name=str(grp)), row=row, col=col)
            else:
                fig.add_trace(go.Scatter(x=sub_df[x], y=sub_df[y],
                                         mode="lines+markers", name=name), row=row, col=col)
        else:
            fig.add_trace(go.Bar(x=sub_df[x], y=sub_df[y], name=name), row=row, col=col)

    fig.update_layout(
        template="plotly_white",
        font=dict(family="Malgun Gothic, NanumGothic, sans-serif", size=11),
        height=420 * rows,
        showlegend=True,
    )

    base64_png = _fig_to_base64_png(fig)
    html_b64 = _fig_to_html_b64(fig)

    result = {"html_path": None, "base64_png": base64_png, "html_b64": html_b64}
    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        html_path = str(out / f"{file_stem}.html")
        _save_html(fig, html_path)
        result["html_path"] = html_path

        if base64_png:
            try:
                png_path = str(out / f"{file_stem}.png")
                fig.write_image(png_path, width=1200, height=420 * rows, scale=1.5)
                result["png_path"] = png_path
            except Exception:
                pass

    return result
