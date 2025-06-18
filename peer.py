import socket
import uuid
import threading
import time
import json
import os
import base64
import zipfile
import subprocess

DISCOVERY_PORT = 9999
WORK_DIR = "work"
TCP_TIMEOUT = 10

peer_id = str(uuid.uuid4())
peer_tcp_port = 11000  # porta TCP usada pelo Peer (pode ser qualquer uma livre)

# ------------------ DISCOVERY MASTER ------------------
def discover_master():
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_sock.settimeout(5)

    msg = {
        "action": "DISCOVER_MASTER",
        "peer_id": peer_id,
        "port_tcp": peer_tcp_port
    }

    udp_sock.sendto(json.dumps(msg).encode(), ('<broadcast>', DISCOVERY_PORT))

    try:
        data, addr = udp_sock.recvfrom(4096)
        response = json.loads(data.decode())
        if response["action"] == "MASTER_ANNOUNCE":
            print(f"[DISCOVERY] Master encontrado: {response['master_ip']}:{response['master_port']}")
            return response["master_ip"], response["master_port"]
    except Exception as e:
        print("[DISCOVERY] Erro ao localizar master:", e)

    return None, None

# ------------------ TCP COMUNICAÇÃO ------------------
def connect_to_master(master_ip, master_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TCP_TIMEOUT)
    sock.connect((master_ip, master_port))
    return sock

def send_json(sock, msg):
    sock.send(json.dumps(msg).encode())
    data = sock.recv(8192)
    return json.loads(data.decode())

# ------------------ REGISTRO E HEARTBEAT ------------------
def heartbeat_loop(master_ip, master_port):
    while True:
        try:
            sock = connect_to_master(master_ip, master_port)
            response = send_json(sock, {
                "action": "HEARTBEAT",
                "peer_id": peer_id
            })
            print(f"[HEARTBEAT] {response['status']}")
            sock.close()
        except Exception as e:
            print("[HEARTBEAT] Falha:", e)
        time.sleep(10)

# ------------------ EXECUTAR TAREFA ------------------
def executar_task(zip_data, task_name):
    os.makedirs(WORK_DIR, exist_ok=True)
    task_path = os.path.join(WORK_DIR, task_name)
    with open(task_path, "wb") as f:
        f.write(zip_data)

    extract_dir = os.path.join(WORK_DIR, task_name + "_dir")
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(task_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Executar main.py
    try:
        result = subprocess.run(
            ["python", "main.py"],
            cwd=extract_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20
        )

        with open(os.path.join(extract_dir, "stdout.txt"), "wb") as f:
            f.write(result.stdout)

        with open(os.path.join(extract_dir, "stderr.txt"), "wb") as f:
            f.write(result.stderr)

        # Compactar resultado
        result_zip_path = os.path.join(WORK_DIR, "result_" + task_name)
        with zipfile.ZipFile(result_zip_path, "w") as zipf:
            zipf.write(os.path.join(extract_dir, "stdout.txt"), "stdout.txt")
            zipf.write(os.path.join(extract_dir, "stderr.txt"), "stderr.txt")

        with open(result_zip_path, "rb") as f:
            return base64.b64encode(f.read()).decode()

    except Exception as e:
        print("[EXEC] Erro ao executar main.py:", e)
        return None

# ------------------ CICLO PRINCIPAL ------------------
def main():
    master_ip, master_port = discover_master()
    if not master_ip:
        print("[MAIN] Master não encontrado.")
        return

    try:
        sock = connect_to_master(master_ip, master_port)

        # REGISTER
        send_json(sock, {
            "action": "REGISTER",
            "peer_id": peer_id,
            "addr": [socket.gethostbyname(socket.gethostname()), peer_tcp_port]
        })
        sock.close()

        # Start heartbeat
        threading.Thread(target=heartbeat_loop, args=(master_ip, master_port), daemon=True).start()

        while True:
            sock = connect_to_master(master_ip, master_port)
            resp = send_json(sock, {
                "action": "REQUEST_TASK",
                "peer_id": peer_id
            })

            if resp.get("action") == "TASK_PACKAGE":
                print(f"[TASK] Tarefa recebida: {resp['task_name']}")
                zip_data = base64.b64decode(resp["task_data"])
                result_data = executar_task(zip_data, resp["task_name"])
                sock.close()

                if result_data:
                    sock = connect_to_master(master_ip, master_port)
                    send_json(sock, {
                        "action": "SUBMIT_RESULT",
                        "peer_id": peer_id,
                        "result_name": "result_" + resp["task_name"],
                        "result_data": result_data
                    })
                    print(f"[TASK] Resultado enviado: result_{resp['task_name']}")
                    sock.close()
            else:
                print("[TASK] Nenhuma tarefa disponível.")
                sock.close()
                time.sleep(5)

    except Exception as e:
        print("[MAIN] Erro geral:", e)

if __name__ == "__main__":
    main()
