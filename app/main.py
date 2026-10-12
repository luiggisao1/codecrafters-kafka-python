import socket
import threading
import traceback

from app.dto import ClientRequest
from app.handlers import route_request


def recv_request(conn: socket.socket) -> bytes | None:
    data = conn.recv(1024)
    return data if data else None


def handle_client(conn: socket.socket):
    with conn:
        while (data := recv_request(conn)) is not None:
            try:
                print("Received data:", data)
                client_request = ClientRequest.model_validate(data)
                response = route_request(client_request)
            except Exception:
                traceback.print_exc()
                break
            conn.sendall(response)


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.

    # TODO: Uncomment the code below to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        conn, address = server.accept()  # wait for client
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
