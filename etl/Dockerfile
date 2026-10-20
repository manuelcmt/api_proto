FROM python:3.12-slim
WORKDIR /app
COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/bq.py etl/etl.py ./
CMD ["python", "-u", "etl.py"]
