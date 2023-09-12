import argparse
from peer import Peer

port = 23333
buffer_size = 104857600


def _argparse():
    parser = argparse.ArgumentParser(description='CAN201_Project')
    parser.add_argument('--ip', dest='ip', action='store', help='IP address of a peer', required=True)
    return parser.parse_args()


def main():
    parser = _argparse()
    peer = Peer(parser.ip, port, buffer_size)
    peer.sync()


if __name__ == '__main__':
    main()
