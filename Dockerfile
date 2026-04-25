FROM python:3.11-slim

WORKDIR /app

# 시스템 의존성 (kaleido PNG 렌더링용)
RUN apt-get update && apt-get install -y \
    chromium \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 차트 저장 디렉터리
RUN mkdir -p /app/kosis_charts
ENV KOSIS_OUTPUT_DIR=/app/kosis_charts

EXPOSE 8000

CMD ["sh", "-c", "uvicorn server:starlette_app --host 0.0.0.0 --port ${PORT:-8000}"]
