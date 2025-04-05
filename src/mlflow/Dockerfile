FROM python:3.10.16-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN  pip install -r /app/requirements.txt

COPY . .

EXPOSE 5500

