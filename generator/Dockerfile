# syntax=docker/dockerfile:1
# FROM python:3.7-alpine
FROM python:3.8-slim
WORKDIR /code
# RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
# EXPOSE 5000
COPY . .
RUN echo "FINISH configuring container!"
