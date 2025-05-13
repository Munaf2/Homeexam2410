import argparse
import socket
import time
from struct import pack, unpack

# === HEADER ===
header_format = '!IHH'
HEADER_SIZE = 8
SYN_FLAG = 8
ACK_FLAG = 4
FIN_FLAG = 2

def create_packet(seq, flags, win, data):
    header = pack(header_format, seq, flags, win)
    return header + data

def parse_header(header):
    return unpack(header_format, header)

def parse_flags(flags):
    syn = flags & SYN_FLAG
    ack = flags & ACK_FLAG
    fin = flags & FIN_FLAG
    return syn, ack, fin

def timestamp():
    return time.strftime('%H:%M:%S.') + str(time.time()).split('.')[1][:6]

# === CLIENT ===
def run_client(server_ip, server_port, filename, window_size):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(0.4)
    addr = (server_ip, server_port)

    print("Connection Establishment Phase:\n")
    client_socket.sendto(create_packet(0, SYN_FLAG, 0, b''), addr)
    print("SYN packet is sent")

    try:
        msg, _ = client_socket.recvfrom(1472)
    except socket.timeout:
        print("SYN-ACK not received: Connection failed")
        return

    _, flags, recv_window = parse_header(msg[:HEADER_SIZE])
    syn, ack_flag, _ = parse_flags(flags)
    if syn and ack_flag:
        print("SYN-ACK packet is received")
    else:
        print("Unexpected response during handshake")
        return

    client_socket.sendto(create_packet(0, ACK_FLAG, 0, b''), addr)
    print("ACK packet is sent")
    print("Connection established\n")

    try:
        with open(filename, 'rb') as f:
            chunks = [chunk for chunk in iter(lambda: f.read(1464), b"")]
    except FileNotFoundError:
        print(f"ERROR: Filen '{filename}' finnes ikke.")
        return

    print("Data Transfer:\n")
    total_packets = len(chunks)
    base = 1
    next_seq = 1
    window_size = min(window_size, recv_window)

    while base <= total_packets:
        while next_seq < base + window_size and next_seq <= total_packets:
            pkt = create_packet(next_seq, 0, 0, chunks[next_seq - 1])
            client_socket.sendto(pkt, addr)
            window_range = ', '.join(str(i) for i in range(base, next_seq + 1))
            print(f"{timestamp()} -- packet with seq = {next_seq} is sent, sliding window = {{{window_range}}}")
            next_seq += 1

        try:
            msg, _ = client_socket.recvfrom(1472)
            _, flags, ack_num = parse_header(msg[:HEADER_SIZE])
            _, ack_flag, _ = parse_flags(flags)
            if ack_flag:
                print(f"{timestamp()} -- ACK for packet = {ack_num} is received")
                if ack_num >= base:
                    base = ack_num + 1
        except socket.timeout:
            print("Timeout occurred. Resending all packets in window...")
            next_seq = base

    print("\nDATA Finished\n")
    print("Connection Teardown:\n")

    fin_packet = create_packet(0, FIN_FLAG, 0, b'')
    client_socket.sendto(fin_packet, addr)
    print("FIN packet is sent")

    try:
        msg, _ = client_socket.recvfrom(1472)
        _, flags, _ = parse_header(msg[:HEADER_SIZE])
        _, ack_flag, fin = parse_flags(flags)
        if fin and ack_flag:
            print("FIN ACK packet is received")
            print("Connection Closes")
    except socket.timeout:
        print("Timeout waiting for FIN-ACK. Exiting.")

    client_socket.close()

# === SERVER ===
def run_server(bind_ip, bind_port, discard_seq):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((bind_ip, bind_port))
    print("The server is ready to receive")

    expected_seq = 1
    data = {}
    start = None
    discard_done = False

    while True:
        msg, addr = server_socket.recvfrom(1472)
        seq, flags, win = parse_header(msg[:HEADER_SIZE])
        syn, ack_flag, fin = parse_flags(flags)

        if syn and not ack_flag:
            print("SYN packet is received")
            server_socket.sendto(create_packet(0, SYN_FLAG | ACK_FLAG, 15, b''), addr)
            print("SYN-ACK packet is sent")
        elif ack_flag and not syn:
            print("ACK packet is received")
            print("Connection established")
            start = time.time()
        elif fin:
            print("FIN packet is received")
            server_socket.sendto(create_packet(0, FIN_FLAG | ACK_FLAG, 0, b''), addr)
            print("FIN ACK packet is sent")
            break
        elif len(msg) > HEADER_SIZE:
            if seq == discard_seq and not discard_done:
                print(f"{timestamp()} -- packet {seq} is discarded ONCE (testing)")
                discard_done = True
                continue
            if seq == expected_seq:
                print(f"{timestamp()} -- packet {seq} is received")
                data[seq] = msg[HEADER_SIZE:]
                server_socket.sendto(create_packet(0, ACK_FLAG, seq, b''), addr)
                print(f"{timestamp()} -- sending ack for the received {seq}")
                expected_seq += 1
            elif seq < expected_seq:
                print(f"{timestamp()} -- duplicate packet {seq} is received")
                server_socket.sendto(create_packet(0, ACK_FLAG, seq, b''), addr)
                print(f"{timestamp()} -- resending ack for already received {seq}")

    with open("received_file", "wb") as f:
        for i in range(1, expected_seq):
            f.write(data.get(i, b''))

    elapsed = time.time() - start if start else 1
    size = sum(len(d) for d in data.values()) * 8 / 1_000_000
    print(f"The throughput is {size / elapsed:.2f} Mbps")
    print("Connection Closes")

# === MAIN ===
def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--client', action='store_true')
    group.add_argument('-s', '--server', action='store_true')
    parser.add_argument('-i', '--ip', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int, default=8080)
    parser.add_argument('-f', '--file')
    parser.add_argument('-w', '--window', type=int, default=3)
    parser.add_argument('-d', '--discard', type=int, default=-1)
    args = parser.parse_args()

    if args.client:
        if not args.file:
            print("File missing. Use -f <filename>")
            return
        run_client(args.ip, args.port, args.file, args.window)
    elif args.server:
        run_server(args.ip, args.port, args.discard)

if __name__ == '__main__':
    main()
