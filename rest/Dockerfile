FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir flask redis jsonpickle requests minio

COPY rest-server.py .

EXPOSE 5000

CMD ["python", "rest-server.py"]