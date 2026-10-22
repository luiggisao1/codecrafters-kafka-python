import socket
import threading
import traceback
from pathlib import Path
from app.constants import METADATA_PATH
from app.dto import ClientRequest
from app.handlers import route_request
from app.metadata import ClusterMetadata


def recv_request(conn: socket.socket) -> bytes | None:
    data = conn.recv(1024)
    return data if data else None


def handle_client(conn: socket.socket, metadata: ClusterMetadata):
    with conn:
        while (data := recv_request(conn)) is not None:
            try:
                print("Received data:", data)
                client_request = ClientRequest.model_validate(data)
                response = route_request(client_request, metadata)
            except Exception:
                traceback.print_exc()
                break
            conn.sendall(response)


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.

    # TODO: Uncomment the code below to pass the first stage
    #
    metadata_path = Path(METADATA_PATH)
    metadata = (
        ClusterMetadata.from_file(metadata_path) if metadata_path.exists() else None
    )
    print("metadata topics", metadata.topics if metadata else None)
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        conn, address = server.accept()  # wait for client
        threading.Thread(
            target=handle_client, args=(conn, metadata), daemon=True
        ).start()


if __name__ == "__main__":
    main()
