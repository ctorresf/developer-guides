# Use an official Python runtime as a parent image.
FROM python:3.9-slim

# Set the working directory in the container to /app.
WORKDIR /app

# Copy the requirements file into the working directory.
COPY requirements.txt .

# Install the Python packages.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the working directory.
COPY . .

# Expose port 5000, which Flask runs on.
EXPOSE 5000

# Run the command to start the Flask application.
CMD ["python", "app.py"]
