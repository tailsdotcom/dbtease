FROM python:3.9-alpine

# Install gcc
RUN apk add build-base

COPY . .

# Install from requirements (pins dependencies)
RUN pip install -r requirements.txt
RUN pip install -e .
RUN pip install -r dev-requirements.txt
