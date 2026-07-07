# Stage 1: Build React Frontend
FROM registry.access.redhat.com/ubi9/nodejs-18 AS frontend-build
USER root
WORKDIR /app
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Python Backend
FROM registry.access.redhat.com/ubi9/python-39
USER root
WORKDIR /opt/hi-track/ui

# Install backend dependencies
COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY server/ ./

# Copy built frontend from Stage 1
COPY --from=frontend-build /app/dist /var/www/hi-track-alert

# Set environment variable so the backend knows where to find the static files
ENV HITRACK_UI_DIST=/var/www/hi-track-alert

EXPOSE 8000

# Run uvicorn
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
