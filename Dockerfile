FROM python:3.11-slim

# Install OS CA bundle
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Point requests/urllib and Python SSL at a trusted bundle
ENV REQUESTS_CA_BUNDLE=/usr/local/lib/python3.11/site-packages/certifi/cacert.pem
ENV SSL_CERT_FILE=/usr/local/lib/python3.11/site-packages/certifi/cacert.pem

COPY . .

CMD ["bash", "-c", "celery -A tasks worker --beat --concurrency=${CELERY_CONCURRENCY:-10} --loglevel=info"]
