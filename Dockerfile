FROM python:3.9-alpine

RUN pip install pipenv

COPY . .

# Generate requirements
RUN pipenv lock --dev -r > requirements.txt
# Install from requirements
RUN pip install -r requirements.txt
