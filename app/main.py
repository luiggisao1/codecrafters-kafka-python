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
        data = conn.recv(1024)
        message_size = data[0:4]
        correlation_id = data[8:12]
        # message_size = (0).to_bytes(4, byteorder="big", signed=True)
        # correlation_id = (7).to_bytes(4, byteorder="big", signed=True)
        response = message_size + correlation_id
        conn.sendall(response)

if __name__ == "__main__":
    main()
