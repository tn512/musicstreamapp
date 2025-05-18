FROM python:3.9.18-slim

WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy data files
COPY data/ /app/data/

# Copy application code
COPY generator.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a wrapper script to handle environment variables
RUN echo '#!/bin/bash\n\
echo "Starting event generator..."\n\
echo "Kafka bootstrap servers: $KAFKA_BOOTSTRAP_SERVERS"\n\
echo "Contents of /app:"\n\
ls -la /app\n\
echo "Contents of /app/data:"\n\
ls -la /app/data\n\
echo "Python version:"\n\
python -V\n\
\n\
# Test network connectivity to Kafka broker\n\
echo "Testing Kafka connectivity:"\n\
IFS="," read -ra SERVERS <<< "$KAFKA_BOOTSTRAP_SERVERS"\n\
for SERVER in "${SERVERS[@]}"; do\n\
    HOST=$(echo $SERVER | cut -d: -f1)\n\
    PORT=$(echo $SERVER | cut -d: -f2)\n\
    echo "Testing connection to $HOST:$PORT"\n\
    if nc -z -v -w5 $HOST $PORT 2>/dev/null; then\n\
        echo "Successfully connected to $HOST:$PORT"\n\
    else\n\
        echo "Failed to connect to $HOST:$PORT"\n\
    fi\n\
    # Try to resolve the hostname\n\
    echo "Trying to resolve $HOST:"\n\
    python -c "import socket; print(f\"IP address: {socket.gethostbyname(\\\"$HOST\\\")}\")"\n\
done\n\
\n\
echo "Running generator.py with debug enabled..."\n\
python generator.py --kafka-bootstrap-servers=$KAFKA_BOOTSTRAP_SERVERS --debug\n\
' > /app/start.sh && chmod +x /app/start.sh

# Install netcat for network testing
RUN apt-get update && apt-get install -y netcat-openbsd && apt-get clean

# Command to run the event generator
CMD ["/app/start.sh"] 