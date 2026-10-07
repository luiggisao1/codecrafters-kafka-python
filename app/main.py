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
        _ = connection.recv(1024)
        connection.sendall(b"\x00\x00\x00\x04\x00\x00\x00\x07")

if __name__ == "__main__":
    main()
