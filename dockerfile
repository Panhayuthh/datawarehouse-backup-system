FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    python3-dev \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

CMD ["python", "-u", "main.py"]