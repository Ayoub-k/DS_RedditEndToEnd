# Base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the Pipfile and Pipfile.lock to the working directory
COPY Pipfile Pipfile.lock setup.py .env README.md   /app/

# Install dependencies
RUN pip install pipenv && \
    pipenv install --system --deploy --ignore-pipfile

# Copy the necessary folders from the project to the working directory
COPY etl/ /app/etl/
COPY pipelines/ /app/pipelines/
COPY src/ /app/src/
COPY tests/ /app/tests/
COPY config/ /app/config/
COPY docs/  /app/docs
COPY logs/  /app/logs

# Expose the necessary ports
EXPOSE 8080

# Set the entrypoint command
ENTRYPOINT ["airflow"]

# Set the default command for the container
CMD ["webserver"]
