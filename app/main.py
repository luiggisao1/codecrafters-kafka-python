import socket  # noqa: F401
import threading
import traceback
import uuid

from app.dto import (
    ApiVersion,
    ApiVersionsResponse,
    ServerResponse,
    ClientRequest,
    DescribeTopicBodyResponse,
    HeaderResponse,
    TopicResponse,
)


def recv_request(conn: socket.socket) -> bytes | None:
    data = conn.recv(1024)
    return data if data else None


def build_response(client_request: ClientRequest) -> bytes:
    header = HeaderResponse(
        correlation_id=client_request.correlation_id,
        api_version=client_request.api_version,
    )

    if client_request.api_key == 75:
        body = DescribeTopicBodyResponse(
            throttle_time_ms=0,
            topics=[
                TopicResponse(
                    error_code=3,
                    topic_name=client_request.describe_topic_partition.topics[
                        0
                    ].topic_name,
                    partitions=0,
                    topic_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    is_internal=False,
                    topic_authorized_operations=0,
                    tag_buffer=0,
                )
            ],
            next_cursor=-1,
            tag_buffer=0,
        )
    else:
        body = ApiVersionsResponse(
            error_code=0,
            api_keys=[
                ApiVersion(api_key=18, min_version=0, max_version=4, tag_buffer=0),
                ApiVersion(api_key=75, min_version=0, max_version=0, tag_buffer=0),
            ],
            throttle_time_ms=0,
        )

    return ServerResponse(
        header=header,
        body=body,
    ).serialize()


def handle_client(conn: socket.socket):
    with conn:
        while (data := recv_request(conn)) is not None:
            try:
                print(data)
                client_request = ClientRequest.model_validate(data)
                response = build_response(client_request)
            except Exception as e:
                traceback.print_exc()
                break
            # match int.from_bytes(request_api_version, byteorder="big"):
            #     case 4:
            #         api_versions = [ApiVersion(18, 0, 4, 0), ApiVersion(75, 0, 0, 0)]
            #         api_versions_response = ApiVersionsResponse(0, api_versions, 0)
            #         response = ServerResponse(
            #             int.from_bytes(correlation_id, byteorder="big"),
            #             api_versions_response,
            #         ).serialize()
            #
            #     case _:
            #         response = request_message_size + correlation_id + b"\x00\x23"

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
