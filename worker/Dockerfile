FROM xserrat/facebook-demucs:latest

WORKDIR /app

RUN pip3 install --no-cache-dir --upgrade minio redis requests jsonpickle

COPY worker.py .

# Override entrypoint and use unbuffered Python
ENTRYPOINT []
CMD ["python3", "-u", "worker.py"]