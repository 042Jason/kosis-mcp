FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["sh", "-c", "uvicorn server:starlette_app --host 0.0.0.0 --port ${PORT:-8000} --forwarded-allow-ips=* --proxy-headers"]
