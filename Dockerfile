# FPT Shop Laptop Advisor - API image
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python dependencies first to leverage layer caching
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code and data needed at runtime
COPY src ./src
COPY config ./config
COPY data ./data

EXPOSE 8000

# Default dataset path inside the container (can be overridden via env)
ENV LAPTOPS_CSV=data/fpt_laptops_features.csv

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
