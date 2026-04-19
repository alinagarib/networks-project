import socket
import threading
import os
import time
import random
import io
import sys
import struct
import datetime


class Neighbor:
    neighborID: int
    interested: bool       # Is this process interested in this neighbor
    bitfield: list         
    choked: bool           # Is this process choking this neighbor
    downloadSpeed: float   # Bytes downloaded from this neighbor in last interval
    hasFullFile: bool
    isInterested: bool     # Is this neighbor interested in this process 
    bytesDownloaded: int   # Total bytes downloaded from neighbor in current interval
    conn: object           # Socket connection to this neighbor

    def __init__(self, neighborID, interested, bitfield, choked, downloadSpeed, hasFullFile, isInterested=False, conn=None):
        self.neighborID = neighborID
        self.interested = interested
        self.bitfield = bitfield
        self.choked = choked
        self.downloadSpeed = downloadSpeed
        self.hasFullFile = hasFullFile
        self.isInterested = isInterested
        self.bytesDownloaded = 0
        self.conn = conn
        self.lock = threading.Lock()
        self.pendingPiece = None
        self.pendingPieceTime = None


class Peer:
    def __init__(self, id, hostname, port, hasFile):
        self.id = id
        self.hostname = hostname
        self.port = port
        self.hasFile = hasFile

    id: int
    hostname: str
    port: int
    hasFile: bool


class Common:
    def __init__(self, NumberOfPreferredNeighbors, UnchokingInterval,
                 OptimisticUnchokingInterval, FileName,
                 FileSize, PieceSize, NumberOfPieces):
        self.NumberOfPreferredNeighbors = NumberOfPreferredNeighbors
        self.UnchokingInterval = UnchokingInterval
        self.OptimisticUnchokingInterval = OptimisticUnchokingInterval
        self.FileSize = FileSize
        self.FileName = FileName
        self.PieceSize = PieceSize
        self.NumberOfPieces = NumberOfPieces

    NumberOfPreferredNeighbors: int
    UnchokingInterval: int
    OptimisticUnchokingInterval: int  # seconds
    FileSize: int                      # bytes
    FileName: str
    PieceSize: int
    NumberOfPieces: int



def readConfigFile():
    NumberOfPreferredNeighbors = UnchokingInterval = OptimisticUnchokingInterval = None
    FileName = None
    FileSize = PieceSize = None

    with open("Common.cfg", "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("NumberOfPreferredNeighbors"):
                NumberOfPreferredNeighbors = int(line.split()[1])
            elif line.startswith("UnchokingInterval"):
                UnchokingInterval = int(line.split()[1])
            elif line.startswith("OptimisticUnchokingInterval"):
                OptimisticUnchokingInterval = int(line.split()[1])
            elif line.startswith("FileName"):
                FileName = line.split()[1]
            elif line.startswith("FileSize"):
                FileSize = int(line.split()[1])
            elif line.startswith("PieceSize"):
                PieceSize = int(line.split()[1])

    NumberOfPieces = (FileSize + PieceSize - 1) // PieceSize
    config = Common(NumberOfPreferredNeighbors, UnchokingInterval,
                    OptimisticUnchokingInterval, FileName, FileSize,
                    PieceSize, NumberOfPieces)
    return config


def getPeerInfo():
    peers = []
    with open("PeerInfo.cfg", 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            peer = Peer(int(parts[0]), parts[1], int(parts[2]), parts[3] == '1')
            peers.append(peer)
    return peers



message_types = {
    'chokeType': 0,
    'unchokeType': 1,
    'interestedType': 2,
    'notInterestedType': 3,
    'haveType': 4,
    'bitfieldType': 5,
    'requestType': 6,
    'pieceType': 7,
}
message_type_names = {v: k for k, v in message_types.items()}



def makeHandshake(peer_id):
    header = b'P2PFILESHARINGPROJ'
    zeros = b'\x00' * 10
    return header + zeros + struct.pack(">I", peer_id)


def readHandshake(data):
    if len(data) < 32:
        raise ValueError("Handshake too short")
    header = data[:18]
    if header != b'P2PFILESHARINGPROJ':
        raise ValueError("Invalid handshake header")
    peer_id = struct.unpack(">I", data[28:32])[0]
    return peer_id


def makeMessage(messageType, payload=b''):
    messageLength = 1 + len(payload)
    return struct.pack(">I", messageLength) + struct.pack("B", messageType) + payload


def makeChokeMessage():
    return makeMessage(message_types['chokeType'])


def makeUnchokeMessage():
    return makeMessage(message_types['unchokeType'])


def makeInterestedMessage():
    return makeMessage(message_types['interestedType'])


def makeNotInterestedMessage():
    return makeMessage(message_types['notInterestedType'])


def makeHaveMessage(pieceIndex):
    return makeMessage(message_types['haveType'], struct.pack(">I", pieceIndex))


def makeBitfieldMessage(bitfieldBytes):
    return makeMessage(message_types['bitfieldType'], bitfieldBytes)


def makeRequestMessage(pieceIndex):
    return makeMessage(message_types['requestType'], struct.pack(">I", pieceIndex))


def makePieceMessage(pieceIndex, pieceData):
    return makeMessage(message_types['pieceType'], struct.pack(">I", pieceIndex) + pieceData)



def makeBitfieldBytes(NumberOfPieces, haveAll=False):
    numBytes = (NumberOfPieces + 7) // 8
    if haveAll:
        bf = bytearray(b'\xff' * numBytes)
        leftoverBits = numBytes * 8 - NumberOfPieces
        if leftoverBits:
            bf[-1] = bf[-1] & (0xFF << leftoverBits)
    else:
        bf = bytearray(numBytes)
    return bytes(bf)


def hasPiece(bitfieldBytes, pieceIndex):
    byteIndex = pieceIndex // 8
    bitIndex = 7 - (pieceIndex % 8)
    return bool(bitfieldBytes[byteIndex] & (1 << bitIndex))


def setPiece(bitfieldBytes, pieceIndex):
    bf = bytearray(bitfieldBytes)
    byteIndex = pieceIndex // 8
    bitIndex = 7 - (pieceIndex % 8)
    bf[byteIndex] |= (1 << bitIndex)
    return bytes(bf)


def bitfieldToBoolList(bitfieldBytes, numPieces):
    return [hasPiece(bitfieldBytes, i) for i in range(numPieces)]


def boolListToBitfieldBytes(boolList):
    numPieces = len(boolList)
    numBytes = (numPieces + 7) // 8
    bf = bytearray(numBytes)
    for i, has in enumerate(boolList):
        if has:
            byteIndex = i // 8
            bitIndex = 7 - (i % 8)
            bf[byteIndex] |= (1 << bitIndex)
    return bytes(bf)



def recvAll(conn, length):
    data = b''
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            raise ConnectionError("Connection closed mid-receive")
        data += chunk
    return data


def recvMessage(conn):
    raw_len = recvAll(conn, 4)
    msg_len = struct.unpack(">I", raw_len)[0]   
    raw_type = recvAll(conn, 1)
    msg_type = struct.unpack("B", raw_type)[0]
    payload = b''
    if msg_len > 1:
        payload = recvAll(conn, msg_len - 1)
    return msg_type, payload


log_locks = {}
log_lock_global = threading.Lock()


def get_log_lock(peer_id):
    with log_lock_global:
        if peer_id not in log_locks:
            log_locks[peer_id] = threading.Lock()
        return log_locks[peer_id]


def write_log(peer_id, message):
    log_file = f'log_peer_{peer_id}.log'
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}]: {message}\n"
    lock = get_log_lock(peer_id)
    with lock:
        with open(log_file, 'a') as f:
            f.write(log_line)



def getPiecePath(peer_id, piece_index):
    dir_path = f'peer_{peer_id}'
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f'piece_{piece_index}')


def savePiece(peer_id, piece_index, data):
    path = getPiecePath(peer_id, piece_index)
    with open(path, 'wb') as f:
        f.write(data)


def loadPiece(peer_id, piece_index):
    path = getPiecePath(peer_id, piece_index)
    with open(path, 'rb') as f:
        return f.read()


def assembleFile(peer_id, common):
    out_path = os.path.join(f'peer_{peer_id}', common.FileName)
    with open(out_path, 'wb') as out_f:
        for i in range(common.NumberOfPieces):
            out_f.write(loadPiece(peer_id, i))
    print(f"[Peer {peer_id}] Assembled complete file: {out_path}")

def cleanupPieces(peer_id, common):
    for i in range(common.NumberOfPieces):
        path = getPiecePath(peer_id, i)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    print(f"[Peer {peer_id}] Cleaned up {common.NumberOfPieces} piece files.")


def splitFileIntoPieces(peer_id, common):
    src_path = os.path.join(f'peer_{peer_id}', common.FileName)
    if not os.path.exists(src_path):
        raise FileNotFoundError(
            f"Source file not found: '{src_path}' — "
            f"place '{common.FileName}' inside 'peer_{peer_id}/' before starting."
        )
    with open(src_path, 'rb') as f:
        for i in range(common.NumberOfPieces):
            piece_path = getPiecePath(peer_id, i)
            data = f.read(common.PieceSize)
            if not os.path.exists(piece_path):
                savePiece(peer_id, i, data)
    print(f"[Peer {peer_id}] All {common.NumberOfPieces} pieces ready in peer_{peer_id}/")


class PeerState:
    def __init__(self, peer, common, all_peers):
        self.peer = peer
        self.common = common
        self.all_peers = all_peers
        self.allReady = False
        self._lastConnectTime = None
        self.completedPeers = set()

        self.lock = threading.Lock()

        self.myBitfield = makeBitfieldBytes(common.NumberOfPieces, haveAll=peer.hasFile)

        self.neighbors = {}

        self.requestedPieces = set()

        self.optimisticNeighbor = None

        self.downloadComplete = peer.hasFile

        self.expectedNeighborCount = len(all_peers) - 1

    def allConnected(self):
        with self.lock:
            return len(self.neighbors) >= self.expectedNeighborCount

    def piecesNeeded(self, neighborBitfield):
        needed = []
        for i in range(self.common.NumberOfPieces):
            if not hasPiece(self.myBitfield, i) and neighborBitfield[i] and i not in self.requestedPieces:
                needed.append(i)
        return needed

    def countPiecesHave(self):
        return sum(hasPiece(self.myBitfield, i) for i in range(self.common.NumberOfPieces))

    def allPeersComplete(self):
        with self.lock:
            if not self.downloadComplete:
                return False
            for nbr in self.neighbors.values():
                if not nbr.hasFullFile and nbr.neighborID not in self.completedPeers:
                    return False
            expected_ids = {p.id for p in self.all_peers if p.id != self.peer.id}
            return expected_ids.issubset(
                {n.neighborID for n in self.neighbors.values() if n.hasFullFile}
                | self.completedPeers
            )

    def setReadyIfAllConnected(self):
        import time as _time
        self._lastConnectTime = _time.monotonic()
    
    def isReady(self):
        with self.lock:
            if self.allReady:
                return True
            if not self.neighbors:
                return False
            if self._lastConnectTime is None:
                return False
            if time.monotonic() - self._lastConnectTime >= 2.0:
                self.allReady = True
                return True
            return False


def preferred_neighbor_timer(state: PeerState):
    k = state.common.NumberOfPreferredNeighbors
    interval = state.common.UnchokingInterval

    while not state.allPeersComplete():
        time.sleep(interval)

        with state.lock:
            candidates = [n for n in state.neighbors.values() if n.isInterested]

            if not candidates:
                continue

            if state.downloadComplete:
                preferred = random.sample(candidates, min(k, len(candidates)))
            else:
                random.shuffle(candidates)
                candidates.sort(key=lambda n: n.downloadSpeed, reverse=True)
                preferred = candidates[:k]

            preferred_ids = {n.neighborID for n in preferred}

            for n in state.neighbors.values():
                n.downloadSpeed = n.bytesDownloaded / interval if interval > 0 else 0
                n.bytesDownloaded = 0

            for n in state.neighbors.values():
                if n.neighborID in preferred_ids:
                    if n.choked:
                        n.choked = False
                        try:
                            n.conn.sendall(makeUnchokeMessage())
                        except Exception:
                            pass
                elif n.neighborID != state.optimisticNeighbor:
                    if not n.choked:
                        n.choked = True
                        try:
                            n.conn.sendall(makeChokeMessage())
                        except Exception:
                            pass

        write_log(state.peer.id,
                  f"Peer {state.peer.id} has the preferred neighbors "
                  f"{', '.join(str(i) for i in sorted(preferred_ids))}.")



def optimistic_unchoke_timer(state: PeerState):
    interval = state.common.OptimisticUnchokingInterval

    while not state.allPeersComplete():
        time.sleep(interval)

        with state.lock:
            candidates = [n for n in state.neighbors.values()
                          if n.choked and n.isInterested]

            if not candidates:
                continue

            chosen = random.choice(candidates)
            state.optimisticNeighbor = chosen.neighborID
            chosen.choked = False
            try:
                chosen.conn.sendall(makeUnchokeMessage())
            except Exception:
                pass

        write_log(state.peer.id,
                  f"Peer {state.peer.id} has the optimistically unchoked neighbor "
                  f"{chosen.neighborID}.")



def main_neighbor_loop(conn, state: PeerState, neighbor: Neighbor):
    peer_id = state.peer.id
    n_id = neighbor.neighborID

    while True:
        try:
            msg_type, payload = recvMessage(conn)
        except Exception as e:
            write_log(peer_id, f"Peer {peer_id} lost connection with peer {n_id}: {e}")
            break

        if msg_type == message_types['chokeType']:
            write_log(peer_id, f"Peer {peer_id} is choked by {n_id}.")
            with state.lock:
                if neighbor.pendingPiece is not None:
                    state.requestedPieces.discard(neighbor.pendingPiece)
                    neighbor.pendingPiece = None

        elif msg_type == message_types['unchokeType']:
            write_log(peer_id, f"Peer {peer_id} is unchoked by {n_id}.")
            _send_request_if_needed(conn, state, neighbor)

        elif msg_type == message_types['interestedType']:
            write_log(peer_id,
                      f"Peer {peer_id} received the 'interested' message from {n_id}.")
            with state.lock:
                neighbor.isInterested = True

        elif msg_type == message_types['notInterestedType']:
            write_log(peer_id,
                      f"Peer {peer_id} received the 'not interested' message from {n_id}.")
            with state.lock:
                neighbor.isInterested = False

        elif msg_type == message_types['haveType']:
            if len(payload) < 4:
                continue
            piece_index = struct.unpack(">I", payload[:4])[0]
            write_log(peer_id,
                      f"Peer {peer_id} received the 'have' message from {n_id} "
                      f"for the piece {piece_index}.")
            with state.lock:
                neighbor.bitfield[piece_index] = True
                neighbor.hasFullFile = all(neighbor.bitfield)
                if neighbor.hasFullFile:
                    state.completedPeers.add(n_id)
                needed = state.piecesNeeded(neighbor.bitfield)
            if needed:
                conn.sendall(makeInterestedMessage())
                with state.lock:
                    neighbor.interested = True
            else:
                conn.sendall(makeNotInterestedMessage())
                with state.lock:
                    neighbor.interested = False

        elif msg_type == message_types['bitfieldType']:
            with state.lock:
                neighbor.bitfield = bitfieldToBoolList(payload, state.common.NumberOfPieces)
                neighbor.hasFullFile = all(neighbor.bitfield)
                if neighbor.hasFullFile:
                    state.completedPeers.add(n_id)
                needed = state.piecesNeeded(neighbor.bitfield)
            if needed:
                conn.sendall(makeInterestedMessage())
                with state.lock:
                    neighbor.interested = True
            else:
                conn.sendall(makeNotInterestedMessage())
                with state.lock:
                    neighbor.interested = False

        elif msg_type == message_types['requestType']:
            if len(payload) < 4:
                continue
            piece_index = struct.unpack(">I", payload[:4])[0]
            ready = state.isReady()
            with state.lock:
                we_have = hasPiece(state.myBitfield, piece_index)
                not_choked = not neighbor.choked 
            if we_have and not_choked and ready:
                try:
                    piece_data = loadPiece(peer_id, piece_index)
                    conn.sendall(makePieceMessage(piece_index, piece_data))
                except Exception as e:
                    write_log(peer_id,
                              f"Peer {peer_id} failed to send piece {piece_index} "
                              f"to {n_id}: {e}")

        elif msg_type == message_types['pieceType']:
            if len(payload) < 4:
                continue
            piece_index = struct.unpack(">I", payload[:4])[0]
            piece_data = payload[4:]
            write_log(peer_id,
                      f"Peer {peer_id} has downloaded the piece {piece_index} "
                      f"from {n_id}. Now the number of pieces it has is "
                      f"{state.countPiecesHave() + 1}.")

            savePiece(peer_id, piece_index, piece_data)
            with state.lock:
                state.myBitfield = setPiece(state.myBitfield, piece_index)
                state.requestedPieces.discard(piece_index)
                neighbor.pendingPiece = None  
                neighbor.bytesDownloaded += len(piece_data)
                have_all = state.countPiecesHave() == state.common.NumberOfPieces

            _broadcast_have(state, piece_index)

            if have_all and not state.downloadComplete:
                with state.lock:
                    state.downloadComplete = True
                write_log(peer_id, f"Peer {peer_id} has downloaded the complete file.")
                assembleFile(peer_id, state.common)
            else:
                _send_request_if_needed(conn, state, neighbor)

    with state.lock:
        state.neighbors.pop(n_id, None)
    conn.close()



def _send_request_if_needed(conn, state: PeerState, neighbor: Neighbor):
    if not state.isReady():
        return
    with state.lock:
        if (neighbor.pendingPiece is not None and
                neighbor.pendingPieceTime is not None and
                time.monotonic() - neighbor.pendingPieceTime > 30):
            state.requestedPieces.discard(neighbor.pendingPiece)
            neighbor.pendingPiece = None
            neighbor.pendingPieceTime = None
        if neighbor.pendingPiece is not None:
            return  
        needed = state.piecesNeeded(neighbor.bitfield)
        if not needed:
            return
        piece_index = random.choice(needed)
        state.requestedPieces.add(piece_index)
        neighbor.pendingPiece = piece_index
        neighbor.pendingPieceTime = time.monotonic()

    try:
        conn.sendall(makeRequestMessage(piece_index))
    except Exception as e:
        with state.lock:
            state.requestedPieces.discard(piece_index)
            neighbor.pendingPiece = None
        write_log(state.peer.id,
                  f"Peer {state.peer.id} failed to send request for piece "
                  f"{piece_index} to {neighbor.neighborID}: {e}")


def _broadcast_have(state: PeerState, piece_index: int):
    msg = makeHaveMessage(piece_index)
    with state.lock:
        conns = [(n.neighborID, n.conn) for n in state.neighbors.values() if n.conn]
    for n_id, conn in conns:
        try:
            conn.sendall(msg)
        except Exception as e:
            write_log(state.peer.id,
                      f"Peer {state.peer.id} failed to broadcast have({piece_index}) "
                      f"to {n_id}: {e}")




def handle_connection(conn, state: PeerState, expected_id=None):
    peer_id = state.peer.id
    is_outgoing = expected_id is not None

    try:
        conn.sendall(makeHandshake(peer_id))
        hs_data = recvAll(conn, 32)
        n_id = readHandshake(hs_data)

        if is_outgoing and n_id != expected_id:
            raise ValueError(f"Expected peer {expected_id} but got {n_id}")

        if is_outgoing:
            write_log(peer_id, f"Peer {peer_id} makes a connection to Peer {n_id}.")
        else:
            write_log(peer_id, f"Peer {peer_id} is connected from Peer {n_id}.")
        with state.lock:
            my_bf = state.myBitfield
            num_pieces = state.common.NumberOfPieces

        have_any = any(hasPiece(my_bf, i) for i in range(num_pieces))
        neighbor_bitfield = [False] * num_pieces

        if is_outgoing:
            if have_any:
                conn.sendall(makeBitfieldMessage(my_bf))
            conn.settimeout(3.0)
            try:
                msg_type, payload = recvMessage(conn)
                if msg_type == message_types['bitfieldType']:
                    neighbor_bitfield = bitfieldToBoolList(payload, num_pieces)
            except socket.timeout:
                pass 
            finally:
                conn.settimeout(None)
        else:
            if have_any:
                conn.sendall(makeBitfieldMessage(my_bf))
            
            

        neighbor = Neighbor(
            neighborID=n_id,
            interested=False,
            bitfield=neighbor_bitfield,
            choked=True,
            downloadSpeed=0.0,
            hasFullFile=all(neighbor_bitfield),
            isInterested=False,
            conn=conn,
        )

        with state.lock:
            if n_id in state.neighbors:
                print(f"[Peer {peer_id}] Duplicate connection to {n_id}, dropping.")
                conn.close()
                return
            state.neighbors[n_id] = neighbor
            state.setReadyIfAllConnected()
            if neighbor.hasFullFile:
                state.completedPeers.add(n_id)
            needed = state.piecesNeeded(neighbor_bitfield)

        if needed:
            conn.sendall(makeInterestedMessage())
            with state.lock:
                neighbor.interested = True
        else:
            conn.sendall(makeNotInterestedMessage())

    except Exception as e:
        write_log(peer_id, f"Peer {peer_id} handshake failed with peer "
                  f"{expected_id or '?'}: {e}")
        conn.close()
        return

    main_neighbor_loop(conn, state, neighbor)



def accept_loop(server_sock: socket.socket, state: PeerState):
    peer_id = state.peer.id
    while not state.allPeersComplete():
        try:
            server_sock.settimeout(5.0)
            conn, addr = server_sock.accept()
            print(f"[Peer {peer_id}] Accepted incoming connection from {addr}")
            threading.Thread(
                target=handle_connection,
                args=(conn, state),  
                daemon=True
            ).start()
        except socket.timeout:
            continue
        except OSError:
            break
        except Exception as e:
            write_log(peer_id, f"Peer {peer_id} accept error: {e}")
            break
    print(f"[Peer {peer_id}] Accept loop exiting.")



def connect_to_earlier_peers(state: PeerState, all_peers: list):
    peer_id = state.peer.id
    for p in all_peers:
        if p.id == peer_id:
            break   
        connected = False
        for attempt in range(10):
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.connect((p.hostname, p.port))
                threading.Thread(
                    target=handle_connection,
                    args=(conn, state, p.id),
                    daemon=True
                ).start()
                connected = True
                break
            except ConnectionRefusedError:
                conn.close()
                print(f"[Peer {peer_id}] Peer {p.id} not ready, "
                      f"retrying ({attempt + 1}/10)…")
                time.sleep(1)
            except Exception as e:
                conn.close()
                write_log(peer_id,
                          f"Peer {peer_id} failed to connect to Peer {p.id}: {e}")
                break
        if not connected:
            write_log(peer_id,
                      f"Peer {peer_id} gave up connecting to Peer {p.id}.")



def run_peer(peer: Peer, common: Common, all_peers: list):
    state = PeerState(peer, common, all_peers)

    if peer.hasFile:
        try:
            splitFileIntoPieces(peer.id, common)
            print(f"[Peer {peer.id}] Split file into "
                  f"{common.NumberOfPieces} pieces.")
        except FileNotFoundError:
            print(f"[Peer {peer.id}] WARNING: source file not found in "
                  f"peer_{peer.id}/ — make sure it is placed there before "
                  f"starting this peer.")
        except Exception as e:
            print(f"[Peer {peer.id}] WARNING: could not split file: {e}")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((peer.hostname, peer.port))
    server_sock.listen(20)
    print(f"[Peer {peer.id}] Listening on {peer.hostname}:{peer.port}")

    accept_thread = threading.Thread(
        target=accept_loop,
        args=(server_sock, state),
        daemon=True
    )
    accept_thread.start()

    connect_to_earlier_peers(state, all_peers)

    pn_thread = threading.Thread(
        target=preferred_neighbor_timer,
        args=(state,),
        daemon=True
    )
    ou_thread = threading.Thread(
        target=optimistic_unchoke_timer,
        args=(state,),
        daemon=True
    )
    pn_thread.start()
    ou_thread.start()

    print(f"[Peer {peer.id}] Running — waiting for all "
          f"{state.expectedNeighborCount} peers to connect…")
    
    while not state.isReady():
        time.sleep(0.1)

    with state.lock:
        unchoked_neighbors = [(n.conn, n) for n in state.neighbors.values() if not n.choked]
    for conn, neighbor in unchoked_neighbors:
        _send_request_if_needed(conn, state, neighbor)

    while not state.allPeersComplete():
        time.sleep(2)
        with state.lock:
            connected = len(state.neighbors)
            expected = state.expectedNeighborCount
        if connected < expected:
            print(f"[Peer {peer.id}] Connected to {connected}/{expected} peers, "
                  f"waiting for more…")

    server_sock.close()
    with state.lock:
        conns = [n.conn for n in state.neighbors.values() if n.conn]
    for conn in conns:
        try:
            conn.close()
        except Exception:
            pass
    print(f"[Peer {peer.id}] All peers have the complete file. "
          f"Process terminating.")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python peerProcess.py <peerID>")
        sys.exit(1)

    my_peer_id = int(sys.argv[1])

    common = readConfigFile()
    all_peers = getPeerInfo()

    my_peer = next((p for p in all_peers if p.id == my_peer_id), None)
    if my_peer is None:
        print(f"Peer ID {my_peer_id} not found in PeerInfo.cfg")
        sys.exit(1)

    try:
        run_peer(my_peer, common, all_peers)
    except Exception:
        import traceback
        traceback.print_exc()
        write_log(my_peer_id, f"Peer {my_peer_id} crashed: {traceback.format_exc()}")
