FROM python:3.12-slim
WORKDIR /app
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/bq.py dashboard/app.py ./
CMD ["python", "app.py"]
