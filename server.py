import socket
import threading
import json
import random
import time

# Configuration for nodes in the cluster
NODES = [("127.0.0.1", 5551), ("127.0.0.1", 5552), ("127.0.0.1", 5553)]
SERVER_PORT = None  # Set on initialization
NODE_ID = None  # Index for identifying this node in NODES

# Paxos protocol variables
proposal_number = 0
highest_accepted = -1
file_content = None  # Value to simulate "distributed file"
accepted_value = None  # Value to be stored in the file after consensus

# Lock for safe multi-threading
lock = threading.Lock()

# Basic Paxos Protocol
def start_server():
    global SERVER_PORT, NODE_ID
    SERVER_PORT = NODES[NODE_ID][1]

    # Initialize server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", SERVER_PORT))
    server_socket.listen(5)
    print(f"[STARTING] Node {NODE_ID} listening on port {SERVER_PORT}")

    while True:
        client_socket, client_address = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket,)).start()

# Phase 1: Prepare - Proposer requests for acceptance
def prepare_proposal():
    global proposal_number
    proposal_number += 1
    responses = []
    
    for ip, port in NODES:
        if port == SERVER_PORT:
            continue  # Skip self
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                message = json.dumps({"type": "prepare", "proposal_number": proposal_number})
                s.sendall(message.encode())
                response = s.recv(1024).decode()
                responses.append(json.loads(response))
        except ConnectionRefusedError:
            print(f"Node {port} not reachable")
    
    # Check if a majority promised
    if all(r.get("status") == "promise" for r in responses):
        return proposal_number  # Proceed to accept phase
    return None

# Phase 2: Accept - Proposer requests acceptance for the chosen value
def accept_proposal(new_value):
    responses = []
    for ip, port in NODES:
        if port == SERVER_PORT:
            continue  # Skip self
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                message = json.dumps({
                    "type": "accept", 
                    "proposal_number": proposal_number, 
                    "value": new_value
                })
                s.sendall(message.encode())
                response = s.recv(1024).decode()
                responses.append(json.loads(response))
        except ConnectionRefusedError:
            print(f"Node {port} not reachable")

    # Commit if majority accepted
    if sum(1 for r in responses if r.get("status") == "accepted") > len(NODES) // 2:
        with lock:
            global file_content
            file_content = new_value
        print(f"[COMMIT] Node {NODE_ID} updated file content to: {file_content}")
        return True
    return False

# RPC handler for clients
def handle_client(client_socket):
    global highest_accepted, accepted_value
    data = client_socket.recv(1024).decode()
    message = json.loads(data)
    
    if message["type"] == "prepare":
        with lock:
            if message["proposal_number"] > highest_accepted:
                highest_accepted = message["proposal_number"]
                response = {"status": "promise", "proposal_number": highest_accepted}
            else:
                response = {"status": "reject"}
        client_socket.send(json.dumps(response).encode())

    elif message["type"] == "accept":
        with lock:
            if message["proposal_number"] >= highest_accepted:
                highest_accepted = message["proposal_number"]
                accepted_value = message["value"]
                response = {"status": "accepted"}
            else:
                response = {"status": "reject"}
        client_socket.send(json.dumps(response).encode())
    
    elif message["type"] == "SubmitValue":
        # Random delay for conflict scenarios between multiple proposers
        time.sleep(random.uniform(0, 1))  
        
        # Attempt to propose and achieve consensus
        if prepare_proposal():
            if accept_proposal(message["value"]):
                client_socket.send("Consensus achieved: value stored successfully.".encode())
            else:
                client_socket.send("Consensus not achieved.".encode())
        else:
            client_socket.send("Proposal rejected.".encode())

    client_socket.close()

# Entry point for server
if __name__ == "__main__":
    NODE_ID = int(input("Enter this node's ID (0, 1, or 2): "))
    start_server()
