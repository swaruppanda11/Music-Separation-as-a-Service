FROM xserrat/facebook-demucs:latest

WORKDIR /app

RUN pip3 install --no-cache-dir --upgrade minio redis requests jsonpickle

COPY worker.py .

# Use -u flag for unbuffered output
CMD ["python3", "-u", "worker.py"]