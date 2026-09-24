"""chunkstore.py：内容寻址存储（基线：整对象一块，无去重、无回收）。"""
from __future__ import annotations


class ChunkStore:
    def __init__(self, chunk_size: int = 4):
        self.chunk_size = chunk_size
        self.objects = {}
        self.blobs = {}
        self.stored_bytes = 0
        self.dedup_hits = 0
        self.wal = []

    def put(self, name: str, data: bytes) -> dict:
        """基线：整份存一份，同名覆盖。"""
        self.objects[name] = len(data)
        self.blobs[name] = bytes(data)
        self.stored_bytes += len(data)
        self.wal.append(("put", name))
        return {"stored": len(data), "chunks": 1}

    def get(self, name: str):
        return self.blobs.get(name)

    def delete(self, name: str) -> dict:
        """基线：直接丢掉，不回收任何块。"""
        existed = name in self.blobs
        self.blobs.pop(name, None)
        self.objects.pop(name, None)
        return {"deleted": existed, "collected": 0}

    def gc(self) -> dict:
        raise NotImplementedError("引用计数回收还没实现")

    def recover(self) -> dict:
        raise NotImplementedError("重启恢复还没实现")

    def stats(self) -> dict:
        return {"objects": len(self.objects), "chunks": len(self.blobs),
                "stored_bytes": self.stored_bytes, "dedup_hits": self.dedup_hits}
