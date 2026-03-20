FROM python:3.13.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements_engine.txt .
RUN pip install --no-cache-dir -r requirements_engine.txt

COPY . .

CMD ["tail", "-f", "/dev/null"]
