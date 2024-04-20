import socket as _socket
from contextlib import closing


# def check_port_is_open(port, localhost="127.0.0.1"):
#     a_socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)

#     breakpoint()
#     location = (localhost, 80)
#     result_of_check = a_socket.connect_ex(location)
#     try:
#         if result_of_check == 0:
#             out = True
#         else:
#             out = False

#     a_socket.close()
#     return out


def check_port_is_open(host, port):
    with closing(_socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            return True
        else:
            return False
