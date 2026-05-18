###############################################################################
#
#  Version 1.0, Copyright (C) 2026, Aissa Mohammedi <Laval, QC>
#  Protocole d'Invocation Intelligente - IPC 2026
#
#  This program is free software; you can redistribute it and/or
#  modify it under the same terms as the MIT License.
#
#  Inspired by IPC::SysV (1997-2013) - modernized for AI agents
#
###############################################################################

"""
AIPC 2026 - AI Inter-Process Communication

Remplace System V IPC (shmget/semget/msgget) par une couche moderne
pour agents IA. Conçu pour les 5 étapes du protocole :
  1. ECOUTE  -> subscribe()
  2. DETECTION -> memwrite()
  3. FILTRAGE -> lock() / unlock()
  4. NOTIFICATION -> publish()
  5. ACTION -> memread()

Pas de dépendance serveur. Fonctionne avec Python 3.9+ standard library.
"""

import hashlib
import json
import time
from multiprocessing import shared_memory, Semaphore, Queue
from typing import Any, Optional

__version__ = "1.0.0"
__author__ = "Aissa Mohammedi"

# --- CONSTANTS (compatibles esprit SysV) ---
IPC_CREAT = 0o1000
IPC_EXCL = 0o2000
IPC_NOWAIT = 0o4000
IPC_PRIVATE = 0

SHM_RDONLY = 0o10000
SHM_RND = 0o20000

SEM_UNDO = 0x1000

# AI-specific
AI_ECOUTE = "ECOUTE"
AI_DETECTION = "DETECTION"
AI_FILTRAGE = "FILTRAGE"
AI_NOTIFICATION = "NOTIFICATION"
AI_ACTION = "ACTION"

# --- CORE FUNCTIONS ---

def ftok(path: str, proj_id: int = 1) -> str:
    """Génère une clé unique comme l'ancien ftok, mais hash-based."""
    return hashlib.sha256(f"{path}:{proj_id}".encode()).hexdigest()[:16]

class AIChannel:
    def __init__(self, name: str, size: int = 1024*1024, create: bool = True):
        self.key = ftok(name)
        self.size = size
        try:
            if create:
                self.shm = shared_memory.SharedMemory(name=self.key, create=True, size=size)
            else:
                self.shm = shared_memory.SharedMemory(name=self.key)
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=self.key)
        
        self.lock = Semaphore(1)
        self.queue = Queue()

    # System V style
    def shmat(self) -> memoryview:
        return self.shm.buf

    def shmdt(self):
        self.shm.close()

    def memwrite(self, data: Any, pos: int = 0):
        """DETECTION : écrit une intention en mémoire partagée"""
        payload = json.dumps({
            "ts": time.time(),
            "data": data,
            "step": AI_DETECTION
        }).encode()
        end = pos + len(payload)
        if end > self.size:
            raise ValueError("Shared memory overflow")
        self.shm.buf[pos:end] = payload

    def memread(self, pos: int = 0, size: int = 4096) -> dict:
        """ACTION : lit l'intention"""
        raw = bytes(self.shm.buf[pos:pos+size]).rstrip(b'\x00')
        try:
            return json.loads(raw.decode())
        except:
            return {}

    def lock(self, undo: bool = True):
        """FILTRAGE : verrouille pour éviter le bruit concurrent"""
        self.lock.acquire()

    def unlock(self):
        self.lock.release()

    def publish(self, message: Any):
        """NOTIFICATION : envoie aux abonnés"""
        self.queue.put({
            "ts": time.time(),
            "msg": message,
            "step": AI_NOTIFICATION
        })

    def subscribe(self, timeout: Optional[float] = None) -> Any:
        """ECOUTE : attend une notification"""
        try:
            return self.queue.get(timeout=timeout)
        except:
            return None

    def cleanup(self):
        try:
            self.shm.unlink()
        except:
            pass

# --- USAGE EXAMPLE ---
if __name__ == "__main__":
    # Agent 1 : Écoute
    chan = AIChannel("protocole_aissa", create=True)
    
    # Agent 2 : Détection
    chan.lock()
    chan.memwrite({"intent": "utilisateur veut coder", "source": "clavier"})
    chan.publish("DETECTION_COMPLETE")
    chan.unlock()
    
    # Agent 1 : reçoit
    notif = chan.subscribe(timeout=1)
    data = chan.memread()
    print("NOTIF:", notif)
    print("DATA:", data)
    
    chan.cleanup()
