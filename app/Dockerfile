FROM ubuntu:latest
USER root

EXPOSE 8581

WORKDIR /code

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install Python, cron and other packages
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv wget unzip \
    chromium-browser chromium-driver \
    libglib2.0-0 libnss3 libfontconfig1 \
    xvfb libxi6 pulseaudio \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python3 -m venv /code/venv
ENV PATH="/code/venv/bin:$PATH"

# Install Python packages in the virtual environment
RUN pip3 install selenium Pillow pandas beautifulsoup4 tqdm schedule python-multipart bs4 brotli mysql-connector-python requests webdriver-manager

# Set up Chrome environment variables
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=1

# Create necessary directories
RUN mkdir -p /code/app/crawler/logs /code/app/crawler/data

# Copy Python scheduler script
COPY ./crawler/crawler_schedule.py /code/app/crawler/crawler_schedule.py

# Set permissions
RUN chmod +x /code/app/crawler/crawler_schedule.py

# Set up entry point
CMD ["python3", "/code/app/crawler/crawler_schedule.py"]
