FROM python:3.11-slim

# تثبيت ffmpeg (مطلوب لـ yt-dlp)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# تثبيت المكتبات
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ الكود
COPY bot.py .

# مجلد التحميل
RUN mkdir -p /root/instagram_downloads

CMD ["python", "-u", "bot.py"]
