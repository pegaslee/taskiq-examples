FROM python:3.13-slim-bookworm

RUN pip install poetry==2.1.2

# Configuring poetry
RUN poetry config virtualenvs.create false

# Install gcc
RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copying requirements of a project
COPY pyproject.toml poetry.lock /app/src/
WORKDIR /app/src

# Installing requirements
RUN poetry install --only main --no-root

# Copying actuall application
COPY . /app/src/
RUN poetry install --only main 

CMD ["python", "-m", "app"]
