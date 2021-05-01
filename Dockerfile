FROM python:3.9-alpine

RUN pip install pipenv

COPY . .

RUN pipenv install --dev
