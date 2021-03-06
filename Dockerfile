FROM python:3.9

# Set up app directory, copy and move to it
RUN mkdir /app
COPY . /app
WORKDIR /app

# Install from requirements (pins dependencies)
RUN pip install -r requirements.txt
RUN pip install -e .
RUN pip install -r dev-requirements.txt
