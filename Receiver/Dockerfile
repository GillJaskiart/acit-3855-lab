FROM python:3

LABEL maintainer="jgill391@my.bcit.ca"

RUN mkdir /app

# Copy requirements first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy source code
COPY . /app

# Run as non-root user
RUN chown -R nobody:nogroup /app
USER nobody

# Receiver listens on 8080
EXPOSE 8080

# Run the Connexion app
ENTRYPOINT ["python3"]
CMD ["app.py"]