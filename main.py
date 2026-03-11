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
    interested: bool # Are we interested in this neighbor
    bitfield: list[bool] 
    choked: bool # Are we choking this neighbor
    downloadSpeed: float
    hasFullFile: bool
    isInterested: bool
    def __init__(self, neighborID, interested, bitfield, choked, downloadSpeed, hasFullFile, isInterested=False):
        self.neighborID = neighborID
        self.interested = interested
        self.bitfield = bitfield
        self.choked = choked
        self.downloadSpeed = downloadSpeed
        self.hasFullFile = hasFullFile

# Peer info
class Peer:
    def __init__(self, id, hostname, port, hasFile):
        self.id = id
        self.hostname = hostname
        self.port = port
        self.hasFile = hasFile
    
    id:int
    hostname:str
    port:int
    hasFile:bool
    # neighbors: list[Neighbor]

# Common config class
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
    OptimisticUnchokingInterval: int #seconds
    FileSize: int #bytes 
    FileName: str
    PieceSize: int
    NumberOfPieces: int

class PeerState:
    def __init__(self, peer, common, all_peers):
        self.peer = peer
        self.common = common
        self.all_peers = all_peers
        self.lock = threading.Lock()
        self.neighbors = {}
        self.myBitfield = [peer.hasFile] * common.NumberOfPieces
        self.requestedPieces = set()
        self.optimisticNeighbor = None
        self.downLoadComplete = peer.hasFile
        self.ownInfo = next(p for p in all_peers if p.id == peer.id)
        self.bitfield = makeBitfieldBytes(common.NumberOfPieces, haveAll=self.ownInfo.hasFile)
        self.bitfieldLock = threading.Lock()
        self.peer_dir = f"peer_{peer.id}"
        self.pieces = {}
        self.piecesLock = threading.Lock()
        self.preferredNeighbors = set()
        self.optimisticUnchoked = None
        self.chokeLock = threading.Lock()

        self.allDone = False
        
        if self.ownInfo.hasFile:
            self._loadFilePieces()

    def _loadFilePieces(self):
        filePath = os.path.join(self.peer_dir, self.common.FileName)
        if not os.path.exists(filePath):
            print(f"Warning: {filePath} not found")
            return
        with open(filePath, "rb") as f:
            for i in range(self.common.NumberOfPieces):
                pieceData = f.read(self.common.PieceSize)
                if pieceData:
                    self.pieces[i] = pieceData

    def _send(self, state, data):
        with state.lock:
            try:
                state.sock.sendall(data)
            except Exception as e:
                print(f"Send error to {state.neighborID}: {e}")
        
    def _sendChoke(self, state):
        self._send(state, makeMessage(message_types['chokeType']))

    def _sendUnchoke(self, state):
        self._send(state, makeMessage(message_types['unchokeType']))

    # Pieces needed
    
    def piecesNeeded(self, neighborBitField):
        return [i for i in range(self.common.NumberOfPieces) if neighborBitField[i] and not self.myBitfield[i] and i not in self.requestedPieces]

    def countPieces(self):
        return sum(self.myBitfield)
    
    def allPeersComplete(self):
        with self.lock:
            if not self.downLoadComplete:
                return False
            for neighbor in self.neighbors.values():
                if not neighbor.hasFullFile:
                    return False
        return True
    
def preferredNeighborTimer(state: PeerState):
    k = state.common.NumberOfPreferredNeighbors
    interval = state.common.UnchokingInterval
    while not state.allPeersComplete():
        time.sleep(interval)
        with state.lock:
            candidates = [n for n in state.neighbors.values() if n.isInterested]
            if not candidates:
                continue
            if state.downLoadComplete:
                preferred = random.sample(candidates, min(k, len(candidates)))
            else:
                random.shuffle(candidates)
                candidates.sort(key=lambda n: n.downloadSpeed, reverse=True)
                preferred = candidates[:k]
            preferredIDs = set(n.neighborID for n in preferred)
            for n in state.neighbors.values():
                n.downloadSpeed = n.bytesDownloaded / interval if interval > 0 else 0
                n.bytesDownloaded = 0
            for n in state.neighbors.values():
                if n.neighborID in preferredIDs:
                    if n.choked:
                        n.choked = False
                        try:
                            # send unchoke message to n
                            pass
                        except Exception as e:
                            print(f"Peer {state.peer.id} error unchoking peer {n.neighborID}: {e}")
                elif n.neighborID != state.optimisticNeighbor:
                    if not n.choked:
                        n.choked = True
                        try:
                            # send choke message to n
                            pass
                        except Exception as e:
                            print(f"Peer {state.peer.id} error choking peer {n.neighborID}: {e}")
        write_logs(state.peer.id, f"Peer {state.peer.id} preferred neighbors: {', '.join(str(n.neighborID) for n in preferred)}")

def optimisticUnchokeTimer(state: PeerState):
    interval = state.common.OptimisticUnchokingInterval
    while not state.allPeersComplete():
        time.sleep(interval)
        with state.lock:
            candidates = [n for n in state.neighbors.values() if n.isInterested and n.choked and n.neighborID != state.optimisticNeighbor]
            if not candidates:
                continue
            newOptimistic = random.choice(candidates)
            if state.optimisticNeighbor is not None and state.optimisticNeighbor in state.neighbors:
                oldOptimistic = state.neighbors[state.optimisticNeighbor]
                oldOptimistic.choked = True
                try:
                    # send choke message to oldOptimistic
                    pass
                except Exception as e:
                    print(f"Peer {state.peer.id} error choking peer {oldOptimistic.neighborID}: {e}")
            newOptimistic.choked = False
            try:
                # send unchoke message to newOptimistic
                pass
            except Exception as e:
                print(f"Peer {state.peer.id} error unchoking peer {newOptimistic.neighborID}: {e}")
            state.optimisticNeighbor = newOptimistic.neighborID
        write_logs(state.peer.id, f"Peer {state.peer.id} optimistic unchoked peer: {state.optimisticNeighbor}")
        
# File IO helper functions for main_neighbor_loop and peer thread
# Gets the piece path and makes the dir if it doesn't exist
def getPiecePath(peer_id, piece_index):
    dir_path = f"peer_{peer_id}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return os.path.join(dir_path, f"piece_{piece_index}")

# Saves the piece data to the piece path
def savePiece(peer_id, piece_index, piece_data):
    piece_path = getPiecePath(peer_id, piece_index)
    with open(piece_path, 'wb') as f:
        f.write(piece_data)

# Loads the piece data from the piece path
def loadPiece(peer_id, piece_index):
    piece_path = getPiecePath(peer_id, piece_index)
    with open(piece_path, 'rb') as f:
        return f.read()
    
# Assembles the pieces into the original file
def assembleFile(peer_id, common):
    file_path = os.path.join(f"peer_{peer_id}", common.FileName)
    with open(file_path, 'wb') as f:
        for i in range(common.NumberOfPieces):
            piece_data = loadPiece(peer_id, i)
            f.write(piece_data)

# Splits the original file into pieces and saves them
def splitFile(peer_id, common):
    file_path = os.path.join(f"peer_{peer_id}", common.FileName)
    with open(file_path, 'rb') as f:
        for i in range(common.NumberOfPieces):
            piece_data = f.read(common.PieceSize)
            savePiece(peer_id, i, piece_data)

# Config files
def readConfigFile():
    
    with open("Common.cfg", "r") as f:
        # read lines in config file
        for line in f:
            if line.startswith("NumberOfPreferredNeighbors"):
                NumberOfPreferredNeighbors = int(line.split(" ")[1].strip())
            elif line.startswith("UnchokingInterval"):
                UnchokingInterval = int(line.split(" ")[1].strip())
            elif line.startswith("OptimisticUnchokingInterval"):
                OptimisticUnchokingInterval = int(line.split(" ")[1].strip())
            elif line.startswith("FileName"):
                FileName = line.split(" ")[1].strip()
            elif line.startswith("FileSize"):
                FileSize = int(line.split(" ")[1].strip())
            elif line.startswith("PieceSize"):
                PieceSize = int(line.split(" ")[1].strip())
            # Calculate number of pieces
            if FileSize and PieceSize:
                NumberOfPieces = (FileSize + PieceSize - 1) // PieceSize

    config = Common(NumberOfPreferredNeighbors, UnchokingInterval, OptimisticUnchokingInterval,
                    FileName, FileSize, PieceSize, NumberOfPieces)

    return config



def getPeerInfo() -> list[Peer]:
    Peers = []
    with open("PeerInfo.cfg", 'r') as f:
    # read lines from peer info 
        for line in f: 
            parts = line.split(" ").strip()
            peer = Peer()
            peer.id = int(parts[0])
            peer.hostname = parts[1]
            peer.port = int(parts[2])
            peer.hasFile = parts[3] == '1'
            Peers.append(peer)

    return Peers


def createThreads(peers):
    threads = []
    # Change target function
    for peer in peers:
        t = threading.Thread(target=peer_thread, args=(peer,common,))
        threads.append(t)
    
    return threads

# Types of messages and their associated value
message_types = dict(
    chokeType=0,
    unchokeType=1,
    interestedType=2,
    notInterestedType=3,
    haveType=4,
    bitfieldType=5,
    requestType=6,
    pieceType=7
)

# Create the handshake message
def makeHandshake(id):
    HandshakeHeader = "P2PFILESHARINGPROJ"
    HandshakeZeros = "\x00" * 10
    return HandshakeHeader + HandshakeZeros + struct.pack(id)

# Read the handshake message
def readHandshake(data):
    header = data[:18]
    if header != "P2PFILESHARINGPROJ":
        raise ValueError("Invalid handshake header")
    peerID = struct.unpack(">I", data[28:32])[0]
    return peerID

# Make a message
# message length is message type plus payload length, excludes message length field
def makeMessage(messageType, payload):
    if messageType not in message_types:
        raise ValueError(f"Invald message type: {messageType}")
    messageLength = 1 + len(payload)
    return struct.pack(">I", messageLength) + struct.pack("B", messageType) + payload

# Create the have message
def makeHaveMessage(pieceIndex):
    return makeMessage(message_types['haveType'], struct.pack(">I", pieceIndex))

# Create the bitfield message
def makeBitfieldMessage(bitfieldBytes):
    return makeMessage(message_types['bitfieldType'], bitfieldBytes)
# Create the request message
def makeRequestMessage(pieceIndex):
    return makeMessage(message_types['requestType'], struct.pack(">I", pieceIndex))

# Create the piece message
def makePieceMessage(pieceIndex, pieceData):
    return makeMessage(message_types['pieceType'], struct.pack(">I", pieceIndex) + pieceData)

# Create the bitfield bytes
def makeBitfieldBytes(NumberOfPieces, haveAll=False):
    numberofBytes = (NumberOfPieces + 7) // 8
    if haveAll:
        bf = bytearray(b'\xff' * numberofBytes)
        leftoverBits = numberofBytes * 8 - NumberOfPieces
        if leftoverBits:
            bf[-1] = bf[-1] & (0xFF << leftoverBits)
    else:
        bf = bytearray(numberofBytes)
    return bytes(bf)

def hasPiece(bitfield, pieceIndex):
    byteIndex = pieceIndex // 8
    bitIndex = 7 - (pieceIndex % 8)
    return bool(bitfield[byteIndex] & (1 << bitIndex))

def setPiece(bitfield, pieceIndex):
    ba = bytearray(bitfield)
    byteIndex = pieceIndex // 8
    bitIndex = 7 - (pieceIndex % 8)
    ba[byteIndex] |= (1 << bitIndex)
    return bytes(ba)

def countPieces(bitfield):
    return sum(bin(b).count('1') for b in bitfield)

def interestingPieces(ownBitfield, neightborBitfield, numberOfPieces):
    interestingPieces = []
    for i in range(numberOfPieces):
        if not hasPiece(ownBitfield, i) and hasPiece(neightborBitfield, i):
            interestingPieces.append(i)
    return interestingPieces



# Initial peer thread function
def peer_thread(peer, common, Peers):
    # Create a socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((peer.hostname, peer.port))
    s.listen(5)
    # print(f"Peer {peer.id} listening on {peer.hostname}:{peer.port}")
    for p in Peers:
        if p.id != peer.id:
            # Connect to the peer
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.connect((p.hostname, p.port))
                print(f"Peer {peer.id} connected to peer {p.id}")
                write_logs(peer.id, f"Peer {peer.id} makes a connection connected to peer {p.id}")
                # Handle the connection in a new thread
                threading.Thread(target=handle_connection, args=(conn, peer, common, Peers)).start()
            except Exception as e:
                print(f"Peer {peer.id} failed to connect to peer {p.id}: {e}")
                write_logs(peer.id, f"Peer {peer.id} failed to connect to peer {p.id}: {e}" )
    while True:
        conn, addr = s.accept()
        print(f"Peer {peer.id} accepted connection from {addr}")
        # Handle the connection in a new thread
        threading.Thread(target=handle_connection, args=(conn, peer, common, Peers)).start()
    



def handle_connection(conn, peer, common, Peers):
        # Read the handshake message
        data = conn.recv(1024).decode()
        n_id = readHandshake(data)
        # Send handshake response
        handshake_response = makeHandshake(peer.id)
        conn.send(handshake_response.encode())
        # Write the log message
        neighbor = (Neighbor(n_id, False, [0] * common.NumberOfPieces, False, 0.0, False))
        conn.send(makeBitfieldMessage(''.join(['1' if peer.hasFile else '0' for _ in range(common.NumberOfPieces)])).encode())
        n_bitfield = conn.recv(1024).decode()
        neighbor.bitfield = [b == '1' for b in n_bitfield[5:]]
        if(not peer.hasFile and any(neighbor.bitfield)):
            neighbor.interested = True
        neighbor.hasFullFile = neighbor.bitfield.count(True) == common.NumberOfPieces
        main_neighbor_loop(conn, peer, common, Peers, neighbor)
        
# This needs to be changed a lot and the name should be changed, this should handle reading messages from the neighbor of this thread
# Still need to add another funciton that will implement which neighbors are our perferred neighbors.
def main_neighbor_loop(conn, peer, common, Peers, neighbor):
    # peer_id = state.peer.id
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break

            message_length = struct.unpack(">I", data[:4])[0]
            message_type = struct.unpack("B", data[4:5])[0]
            payload = data[5:5+message_length-1]

            if message_type == message_types['interestedType']:
                neighbor.isInterested = True
                print(f"Peer {peer.id} received interested message from peer {neighbor.neighborID}")
                write_logs(peer.id, f"Peer {peer.id} received interested message from peer {neighbor.neighborID}")

            elif message_type == message_types['notInterestedType']:
                neighbor.isInterested = False
                print(f"Peer {peer.id} received not interested message from peer {neighbor.neighborID}")
                write_logs(peer.id, f"Peer {peer.id} received not interested message from peer {neighbor.neighborID}")

            elif message_type == message_types['haveType']:
                pieceIndex = struct.unpack(">I", payload)[0]
                neighbor.bitfield[pieceIndex] = True
                neighbor.hasFullFile = neighbor.bitfield.count(True) == common.NumberOfPieces
                print(f"Peer {peer.id} received have message from peer {neighbor.neighborID} for piece {pieceIndex}")
                write_logs(peer.id, f"Peer {peer.id} received have message from peer {neighbor.neighborID} for piece {pieceIndex}")

            elif message_type == message_types['bitfieldType']:
                neighbor.bitfield = [b == '1' for b in payload]
                neighbor.hasFullFile = neighbor.bitfield.count(True) == common.NumberOfPieces
                print(f"Peer {peer.id} received bitfield message from peer {neighbor.neighborID}")
                write_logs(peer.id, f"Peer {peer.id} received bitfield message from peer {neighbor.neighborID}")
                
            elif message_type == message_types['requestType']:
                pieceIndex = struct.unpack(">I", payload)[0]
                print(f"Peer {peer.id} received request message from peer {neighbor.neighborID} for piece {pieceIndex}")
                write_logs(peer.id, f"Peer {peer.id} received request message from peer {neighbor.neighborID} for piece {pieceIndex}")

            elif message_type == message_types['pieceType']:
                pieceIndex = struct.unpack(">I", payload[:4])[0]
                pieceData = payload[4:]
                print(f"Peer {peer.id} received piece message from peer {neighbor.neighborID} for piece {pieceIndex}")
                write_logs(peer.id, f"Peer {peer.id} received piece message from peer {neighbor.neighborID} for piece {pieceIndex}")
                
                
        except Exception as e:
            print(f"Peer {peer.id} error handling connection with peer {neighbor.neighborID}: {e}")
            write_logs(peer.id, f"Peer {peer.id} error handling connection with peer {neighbor.neighborID}: {e}" )
            break


def write_logs(peer_id, message):
    log_file = f"log_peer_{peer_id}.log"
    timestamp = datetime.datetime.now().strtime("%H:%M:%S")
    log = f"[{timestamp}]: {message}\n"
    
    with open(log_file, 'a') as f:
        f.write(log)


class PeerProcess(peer_id):
    conf = readConfigFile()
    peer_info = getPeerInfo()
    pass

def peerProcess(peer_id):
    conf = readConfigFile()
    peers = getPeerInfo()
    curr_peer = None
    for peer in peers:
        if peer.id == peer_id:
            curr_peer = peer
            break
    if curr_peer == None:
        write_logs(peer_id, f"Peer {peer_id} is not a valid peer id")
    # we need to keep track of the already connected peers, and connect this peer 
    # to the other peer processes 
            
    pass

if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == "peerProcess" and sys.argv[2]:
            peerProcess(sys.argv[2])
        
    common = readConfigFile()
    Peers = getPeerInfo()
    threads = createThreads(Peers)
    for t in threads:
        t.start()
    for t in threads:
        t.join()