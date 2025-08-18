# Use an official Python runtime as a parent image.
# This version includes a small Linux base, which results in a smaller image size.
FROM python:3.9-slim

# Set the working directory in the container to /app.
# This is where all your application code will live.
WORKDIR /app

# Copy the requirements file into the working directory.
COPY requirements.txt .

# Install the required Python packages.
# Using --no-cache-dir helps keep the image size down.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the working directory.
COPY app.py .

# Expose port 8010.
EXPOSE 8010

# Run the command to start the Flask application.
# It makes the app accessible on any IP address, so it can be reached from outside the container.
CMD ["python", "app.py"]
