FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget unzip curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Download pre-built Telegram Local Bot API binary
RUN wget -q \
    "https://github.com/tdlib/telegram-bot-api/releases/latest/download/telegram-bot-api-linux-amd64.zip" \
    -O /tmp/tgapi.zip && \
    unzip /tmp/tgapi.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/telegram-bot-api && \
    rm /tmp/tgapi.zip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x start.sh

RUN mkdir -p /tmp/quran_batch /tmp/tgdata

EXPOSE 7860
CMD ["./start.sh"]
