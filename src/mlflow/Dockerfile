FROM python:3.10.16-slim
WORDDIR /app
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN  pip install --no-cache-dir -r /app/requirements.txt

COPY ..

EXPOSE 5500

