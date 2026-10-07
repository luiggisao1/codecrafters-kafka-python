import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # TODO: Uncomment the code below to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    connection, address = server.accept() # wait for client

    with connection as conn:
        data = connection.recv(1024)
        print(f"received data: {data}")
        request_api_key = data[4:6]
        request_api_version = data[6:8]
        print(f"request api key: {request_api_key}, request api version: {int.from_bytes(request_api_version, byteorder='big')}")
        message_size = data[0:4]
        correlation_id = data[8:12]
        response = message_size + correlation_id + b"\x00\x23"
        conn.sendall(bytes(response))

if __name__ == "__main__":
    main()
