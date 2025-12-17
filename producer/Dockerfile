FROM python:3.9-slim

WORKDIR /app

# Installer les dépendances Python
RUN pip install --no-cache-dir kafka-python requests

# Copier le script
COPY sensor_producer.py .

# Lancer le script
CMD ["python", "-u", "sensor_producer.py"]