# Use a lightweight Python base image. The 'slim-buster' version is ideal
# because it contains only the essentials, reducing the final image size.
# more images in https://hub.docker.com/_/python
FROM python:3.12-slim-trixie

# Set the working directory inside the container.
# All 'RUN', 'CMD', 'COPY', and 'ADD' instructions will execute in this directory.
WORKDIR /app

# Copy the dependencies file (requirements.txt) to the working directory.
# We copy this first to leverage Docker's cache. If this file doesn't change,
# Docker won't re-run the 'pip install' command.
COPY requirements.txt .

# Install all the libraries listed in requirements.txt.
# We use '--no-cache-dir' to prevent pip from storing packages in the cache,
# which helps keep the image smaller.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application's files (everything in the current directory)
# to the working directory inside the container.
COPY . .

# Expose the port that the application will use.
# In this case, port 8000. This is just a documentation directive and
# does not actually publish the port. To publish it, you must use the -p flag with 'docker run'.
EXPOSE 8000

# Define the default command to start the application.
# When the container starts, it will execute this command.
# In this example, the command is 'python app.py'.
CMD ["python", "app.py"]
