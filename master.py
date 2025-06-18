import socket
import threading
import json
import os
import base64

DISCOVERY_PORT = 9999
TCP_PORT = 10000
TASKS_DIR = "tasks"
RESULTS_DIR = "results"
REGISTERED_PEERS = {}

# Cria pastas se não existirem
os.makedirs(TASKS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# ------------------- DISCOVERY VIA UDP -------------------
def udp_discovery_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', DISCOVERY_PORT))
    print(f"[UDP] Aguardando DISCOVER_MASTER na porta {DISCOVERY_PORT}...")
    
    while True:
        data, addr = sock.recvfrom(4096)
        try:
            message = json.loads(data.decode())
            if message["action"] == "DISCOVER_MASTER":
                response = {
                    "action": "MASTER_ANNOUNCE",
                    "master_ip": socket.gethostbyname(socket.gethostname()),
                    "master_port": TCP_PORT
                }
                sock.sendto(json.dumps(response).encode(), addr)
                print(f"[UDP] DISCOVER_MASTER recebido de {addr}, resposta enviada.")
        except Exception as e:
            print("[UDP] Erro:", e)

# ------------------- TCP HANDLER -------------------
def handle_peer(conn, addr):
    peer_id = None

    while True:
        try:
            data = conn.recv(8192)
            if not data:
                break
            msg = json.loads(data.decode())
            action = msg.get("action")

            if action == "REGISTER":
                peer_id = msg["peer_id"]
                REGISTERED_PEERS[peer_id] = msg["addr"]
                conn.send(json.dumps({"status": "REGISTERED"}).encode())
                print(f"[TCP] Peer {peer_id} registrado.")

            elif action == "HEARTBEAT":
                conn.send(json.dumps({"status": "ALIVE"}).encode())
                print(f"[TCP] Heartbeat recebido de {msg['peer_id']}.")

            elif action == "REQUEST_TASK":
                tasks = os.listdir(TASKS_DIR)
                if tasks:
                    task_name = tasks[0]
                    task_path = os.path.join(TASKS_DIR, task_name)
                    with open(task_path, "rb") as f:
                        encoded = base64.b64encode(f.read()).decode()
                    os.remove(task_path)  # remove após enviar
                    response = {
                        "action": "TASK_PACKAGE",
                        "task_name": task_name,
                        "task_data": encoded
                    }
                    conn.send(json.dumps(response).encode())
                    print(f"[TCP] Enviando tarefa {task_name} para {peer_id}.")
                else:
                    print("[TCP] Nenhuma tarefa disponível.")

            elif action == "SUBMIT_RESULT":
                result_name = msg["result_name"]
                result_data = base64.b64decode(msg["result_data"])
                with open(os.path.join(RESULTS_DIR, result_name), "wb") as f:
                    f.write(result_data)
                conn.send(json.dumps({"status": "OK"}).encode())
                print(f"[TCP] Resultado {result_name} recebido de {peer_id}.")

        except Exception as e:
            print(f"[TCP] Erro com {addr}: {e}")
            break

    conn.close()

# ------------------- SERVIDOR TCP -------------------
def tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('', TCP_PORT))
    server.listen()
    print(f"[TCP] Servidor TCP ouvindo na porta {TCP_PORT}...")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_peer, args=(conn, addr), daemon=True).start()

# ------------------- INICIAR -------------------
if __name__ == "__main__":
    threading.Thread(target=udp_discovery_server, daemon=True).start()
    tcp_server()
