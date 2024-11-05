import socket
import json

def submit_value(node_id, value):
    node_ip, node_port = [("127.0.0.1", 5551), ("127.0.0.1", 5552), ("127.0.0.1", 5553)][node_id]
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            message = json.dumps({"type": "SubmitValue", "value": value})
            s.sendall(message.encode())
            response = s.recv(1024).decode()
            print(f"[RESPONSE from Node {node_id}] {response}")
    except Exception as e:
        print(f"[ERROR] Could not connect to Node {node_id}: {e}")

if __name__ == "__main__":
    node_id = int(input("Enter the node ID to submit the value to (0, 1, or 2): "))
    value = input("Enter value to store in CISC5597: ")
    submit_value(node_id, value)

