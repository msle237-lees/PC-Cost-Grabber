FROM python:3.13.7
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
CMD [\
    "python", "data_grabber.py", \
    "--mode", "http", \
    "--base-url", "http://127.0.0.1:8000", \
    "--endpoint-template", "/pcparts/{type}", \
    "--concurrency", "8", \
]