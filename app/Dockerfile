FROM ubuntu:latest
USER root

EXPOSE 8581

WORKDIR /code

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Python, cron, tzdata 및 기타 패키지 설치
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-pip python3-venv wget unzip tzdata \
    chromium-browser chromium-driver \
    libglib2.0-0 libnss3 libfontconfig1 \
    xvfb libxi6 pulseaudio \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 시간대를 한국 표준시(KST)로 설정
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 가상 환경 생성 및 활성화
RUN python3 -m venv /code/venv
ENV PATH="/code/venv/bin:$PATH"

# 가상 환경에 Python 패키지 설치
RUN pip3 install selenium Pillow pandas beautifulsoup4 tqdm schedule python-multipart bs4 brotli mysql-connector-python requests webdriver-manager

# Chrome 환경 변수 설정
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=1

# 필요한 디렉토리 생성
RUN mkdir -p /code/app/crawler/logs /code/app/crawler/data

# Python 스케줄러 스크립트 복사
COPY ./crawler/crawler_schedule.py /code/app/crawler/crawler_schedule.py

# 권한 설정
RUN chmod +x /code/app/crawler/crawler_schedule.py

# 진입점 설정
CMD ["python3", "/code/app/crawler/crawler_schedule.py"]