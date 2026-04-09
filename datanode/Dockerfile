FROM python:3.11-slim

# Important for real-time logs in Docker Desktop
ENV PYTHONUNBUFFERED=1
# Tells Python to look for modules in the /app folder
ENV PYTHONPATH=/app

WORKDIR /app

# Copy requirements from the datanode folder
COPY datanode/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 1. Copy the datanode package
COPY datanode/ ./datanode/

# 2. Copy the proto package (This was missing!)
# Since we build from the root, this grabs the folder outside datanode/
COPY proto/ ./proto/

# Create system folders
RUN mkdir -p /data/chunks /data/tmp /logs

EXPOSE 50052

# Run as a module so imports work perfectly
CMD ["python", "-m", "datanode.app.main"]

