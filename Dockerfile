# Use the lightweight base image suitable for Python 3.12 applications
FROM python:3.12.4

# Environment variables for Python best practices
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory within the container to match your project
WORKDIR /app

# Copy only the essential files to avoid unnecessary rebuilds
COPY Pipfile Pipfile.lock ./

# Install pipenv
RUN pip install pipenv

# Install project dependencies
RUN pipenv install --dev --system --deploy

# Copy the rest of the project files
COPY . .

# Set the default command to run main.py when the container starts
CMD ["python", "main.py"]
