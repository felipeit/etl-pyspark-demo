FROM python:3.12-slim

# Install system build deps required by some Python wheels
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl gcc g++ libpq-dev python3-setuptools && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1

CMD ["bash"]
