import socket  # noqa: F401
import threading

from app.dto import ApiVersion, ApiVersionsResponse, ServerResponse


def recv_request(conn: socket.socket) -> bytes | None:
    data = conn.recv(1024)
    return data if data else None


def handle_client(conn: socket.socket):
    with conn:
        while (data := recv_request(conn)) is not None:
            request_api_key = data[4:6]
            request_api_version = data[6:8]
            request_message_size = data[0:4]
            correlation_id = data[8:12]

            match int.from_bytes(request_api_version, byteorder="big"):
                case 4:
                    api_versions = [ApiVersion(18, 0, 4, 0), ApiVersion(75, 0, 0, 0)]
                    api_versions_response = ApiVersionsResponse(0, api_versions, 0)
                    response = ServerResponse(
                        int.from_bytes(correlation_id, byteorder="big"),
                        api_versions_response,
                    ).serialize()

                case _:
                    response = request_message_size + correlation_id + b"\x00\x23"

            conn.sendall(bytes(response))


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
