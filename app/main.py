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
        data = list(connection.recv(1024))
        print(f"received data: {data}")
        request_api_key = data[4:6]
        request_api_version = data[6:8]
        correlation_id = data[8:12]
        print(f"correlation_id: {correlation_id}")
        # message_size = (0).to_bytes(4, byteorder="big", signed=True)
        # correlation_id = (7).to_bytes(4, byteorder="big", signed=True)
        conn.sendall(bytes(correlation_id))

if __name__ == "__main__":
    main()
