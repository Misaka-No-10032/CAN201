import os
import socket
import json
from struct import Struct, error as struct_error


class Recorder:
    """A Recorder class to help Peer distinguish which files should be sent.

    Create a sync_record.json in cwd with the following structure:
     {
         "path_1": {"ownership": val_1a, "mtime": val_1b},
         "path_2": {"ownership": val_2a, "mtime": val_2b},
     }

    Typical usage example:

    recorder = Recorder()
    recorder.add_rec()
    """

    def __init__(self):
        self.record_file_dir = "./sync_record.json"
        self.share_dir = "./share"
        # Try to read from local record, if no local record, an empty record will be created.
        try:
            with open(self.record_file_dir, "r") as r:
                self.record = json.load(r)
        except (json.JSONDecodeError, FileNotFoundError):
            self.record = {}

    def __is_unsent(self, path: str) -> bool:
        if not os.path.isdir(path):
            if path not in self.record:  # New added files.
                return True
            elif "mtime" in self.record[path].keys():  # Modified files.
                return os.path.getmtime(path) != self.record[path]["mtime"]
            else:
                # The file recorded with no mtime means the connection broken during the transmission,
                # the owner should resend this file.
                return self.record[path]["ownership"]
        else:
            return False

    def set_ownership(self, path: str, val: bool):
        # When a peer starts to send/receive a file, record the ownership to True/False.
        self.record[path] = {"ownership": val}
        with open(self.record_file_dir, "w") as r:
            json.dump(self.record, r)

    def add_rec(self, path: str):
        # After sending/receiving a file, record the mtime of that file.
        self.record[path].update({"mtime": os.stat(path).st_mtime})
        with open(self.record_file_dir, "w") as r:
            json.dump(self.record, r)

    def del_rec(self, path: str):
        # Before receive a
        self.record.pop(path, None)
        with open(self.record_file_dir, "w") as r:
            json.dump(self.record, r)

    def get_unsent_files(self) -> list:
        """Traversing the share folder and return a list of files need to be sent.

        :return: A list of files need to be sent.
        """
        unsent_file_lst = []
        for root, dirs, files in os.walk(self.share_dir, topdown=False):
            files.extend(dirs)
            for name in files:
                path = os.path.join(root, name)
                if self.__is_unsent(path):
                    unsent_file_lst.append(path)
        return unsent_file_lst


class Peer:
    """A Peer class that can running as either a server or a client.

    When a Peer is initialized, a Recorder will be initialized automatically, the Peer
    firstly tries to act as a client, if failed to connect to another peer, it turns to
    act as a server and waiting for connection, if no other peer connect to it within 5 secs
    it will repeat the above steps until a connection is established.

    Typical usage example:

    peer = Peer()
    peer.sync()
    """

    def __init__(self, peer_ip: str, port: int, receive_buffer_size: int):
        self.peer_ip = peer_ip
        self.port = port
        self.receive_buffer_size = receive_buffer_size
        # A recorder to record the information of synchronized files.
        self.recorder = Recorder()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn_sock = None
        # A mode variable to record the running status, 0: initial mode, 1: client mode, 2: server mode
        self.mode = 0
        # header: (length of file path, size of the file)
        self.header = Struct("!II")

        self.__start()

    def __run_as_client(self):
        print(f"[+] Trying to connecting to {self.peer_ip}:{self.port}.")
        try:
            self.sock.connect((self.peer_ip, self.port))
            # The connection socket of a client is the origin socket.
            self.conn_sock = self.sock
            # Turns the mode to client mode (1)
            self.mode = 1
            print(f"[+] Connected to {self.peer_ip}:{self.port}, running as client.")
        except (ConnectionError, OSError):
            print("[+] Peer does not start up, running as server.")

    def __run_as_server(self):
        try:
            self.sock.bind(("", self.port))
            self.sock.listen(2)
            print("[+] Ready to accept connection.")
            self.conn_sock, address = self.sock.accept()
            self.mode = 2
            print(f"[+] Accepted connection from {address[0]}:{address[1]}.")
        except (OSError, socket.timeout):
            print("[+] An Error occurred, trying to resume.")

    def __start(self):
        # Alternate attempts running as client and server until a connection is established.
        while self.mode == 0:
            self.__run_as_client()
            if self.mode != 1:
                self.__run_as_server()

    def __send(self):
        # Get a list of files need to be sent.
        unsent_files = self.recorder.get_unsent_files()
        if unsent_files:
            print("[+] New file(s) found, start to send file(s).")
            for path in unsent_files:
                # Mark the ownership.
                self.recorder.set_ownership(path, True)

                path_bin = path.encode()
                file_size = os.path.getsize(path)

                # header_name: header + path
                header_path = self.header.pack(len(path_bin), file_size) + path_bin
                self.conn_sock.send(header_path)
                print(f"[+] Sending {path}.")
                with open(path, "rb") as f:
                    self.conn_sock.sendfile(f)
                self.recorder.add_rec(path)
        # Using header (0, 0) to tell the receiver all files has been sent.
        self.conn_sock.send(self.header.pack(0, 0))

    def __receive(self):
        # A flag to tell the peer whether to keep receiving or stop.
        more_files = True
        header_len = self.header.size
        while more_files:
            try:
                # Receive the header.
                header_info = self.conn_sock.recv(header_len)
                path_size, file_size = self.header.unpack(header_info)
                if path_size == file_size == 0:
                    more_files = False
                else:
                    # Receive the file path.
                    path = self.conn_sock.recv(path_size).decode()
                    # Mark the ownership.
                    self.recorder.del_rec(path)
                    self.recorder.set_ownership(path, False)
                    received_bytes = 0
                    # If the folder does not exist, create one.
                    if not os.path.exists(path.rsplit(sep="/", maxsplit=1)[0]):
                        os.mkdir(path.rsplit(sep="/", maxsplit=1)[0])
                    print(f"[+] Receiving {path}.")
                    with open(path, "wb") as f:
                        # Receive from buffer until all bytes of the file has been read.
                        while received_bytes < file_size:
                            bytes_read = self.conn_sock.recv(
                                min(
                                    file_size - received_bytes, self.receive_buffer_size
                                )
                            )
                            f.write(bytes_read)
                            received_bytes += len(bytes_read)
                    self.recorder.add_rec(path)
            except struct_error:
                more_files = False

    def resume(self):
        if self.mode == 2:
            # The client shutdown, waiting for the next connection request.
            self.conn_sock, address = self.sock.accept()
            print(f"[+] Accepted connection from {address[0]}:{address[1]}.")
        else:
            # The server shutdown, the client will become a new server.
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.__run_as_server()
            if self.mode != 2:
                # Failed to change to a server, restart the services.
                print("[+] Failed to resume, restart services.")
                self.mode = 0
                self.__start()

    def sync(self):
        while True:
            try:
                if self.mode == 2:
                    # Server receives command from client act the corresponding action.
                    cmd = self.conn_sock.recv(1).decode()
                    if cmd == "s":
                        self.__send()
                    elif cmd == "r":
                        self.__receive()
                elif self.mode == 1:
                    # Client receive files from server.
                    self.conn_sock.send(b"s")  # Let server send files.
                    self.__receive()

                    # Client send files to server.
                    self.conn_sock.send(b"r")  # Let server receive files.
                    self.__send()
            except ConnectionError:
                # One peer is killed, resume from the exception.
                print("[+] Connection broken, trying to resume.")
                self.resume()
