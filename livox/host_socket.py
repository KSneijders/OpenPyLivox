import pathlib
import socket


if 's' not in locals():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def connect(host="127.0.0.1", port=7878):
    s.connect((host, port))


def notify(filename, path=None, c_format="spherical"):
    """Notify the host. Send an absolute filepath, filename, and the coordinate format to the host."""
    if path is None:
        path = pathlib.Path().absolute()
    string = f"path={path}\\{filename}|format={c_format}"
    s.sendall(str.encode(string, "utf-8"))


def close():
    s.close()
