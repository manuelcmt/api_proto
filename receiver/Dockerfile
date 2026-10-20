FROM python:3.12-slim
WORKDIR /app
COPY receiver/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY receiver/receiver.py .
CMD ["uvicorn", "receiver:app", "--host", "0.0.0.0", "--port", "8000"]
