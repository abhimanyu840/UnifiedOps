import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    sock.bind(("0.0.0.0", 162))
    print("SUCCESS: Python is listening on UDP 162!")
    print("Send a test trap from PowerMax now...")
    while True:
        data, addr = sock.recvfrom(2048)
        print(f"!!! RECEIVED {len(data)} bytes from {addr} !!!")
except PermissionError:
    print("ERROR: Permission denied. You must run this as Administrator.")
except OSError as e:
    print(f"ERROR: Cannot bind to port 162. Is another service using it? {e}")
