import socket  # noqa: F401
import threading


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

            match int.from_bytes(request_api_version, byteorder='big'):
                case 4:
                    error_code = b"\x00\x00"
                    api_keys_length = b"\x02"
                    api_key_1 = b"\x00\x12"  # 18
                    min_version = b"\x00\x00"
                    max_version = b"\x00\x04"
                    tag_buffer_1 = b"\x00"
                    throttle_time_ms = bytes(4)  # b"\x00\x00\x00\x00"
                    tag_buffer = bytes(1)
                    response_messages_size = len(
                        correlation_id + error_code + api_keys_length + api_key_1 + min_version + max_version + tag_buffer_1 + throttle_time_ms + tag_buffer).to_bytes(
                        4, byteorder='big', signed=True)

                    response = response_messages_size + correlation_id + error_code + api_keys_length + api_key_1 + min_version + max_version + tag_buffer_1 + throttle_time_ms + tag_buffer
                case _:
                    response = request_message_size + correlation_id + b"\x00\x23"


            conn.sendall(bytes(response))

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # TODO: Uncomment the code below to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, address = server.accept()  # wait for client

    threading.Thread(target=handle_client, args=(conn,)).start()

    handle_client(conn)

if __name__ == "__main__":
    main()
