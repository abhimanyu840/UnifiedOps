FROM registry.access.redhat.com/ubi9/python-39
USER root
WORKDIR /opt/hi-track/listener

# Install dependencies
COPY listener/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all listeners
COPY listener/ ./

# Expose syslog port
EXPOSE 514/udp

# The command will be overridden by the systemd quadlet depending on the listener role
CMD ["python", "syslog_trap_listener_cdvl.py"]
