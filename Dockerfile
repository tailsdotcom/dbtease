FROM python:3.9

RUN && pip install pipenv

COPY . .

RUN pipenv install --dev
