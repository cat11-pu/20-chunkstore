"""chunkstore.py：内容寻址分块存储（去重 + 引用计数 + GC + WAL 恢复）。"""
from __future__ import annotations

import hashlib
import json
import os


class ChunkStore:
    def __init__(self, chunk_size: int = 4, data_dir: str | None = None):
        self.chunk_size = chunk_size
        self.data_dir = data_dir
        self.objects = {}       # name -> [chunk hash, ...]
        self.chunks = {}        # hash -> bytes（唯一块，全局只存一份）
        self.refcounts = {}     # hash -> 引用计数
        self.stored_bytes = 0   # 当前落盘的唯一块总字节
        self.dedup_hits = 0
        self.wal = []           # 内存日志；data_dir 下同时追加到 wal.log
        self._gc_candidates = set()  # delete 减过引用、待 gc 检查的块
        if self.data_dir:
            os.makedirs(os.path.join(self.data_dir, "chunks"), exist_ok=True)

    # ---- 内部：日志 ----

    def _log(self, record):
        self.wal.append(record)
        if self.data_dir:
            line = json.dumps(record).encode() + b"\n"
            with open(self._wal_path(), "ab") as fh:
                fh.write(line)
                fh.flush()

    def _wal_path(self):
        return os.path.join(self.data_dir, "wal.log")

    def _chunk_path(self, digest):
        return os.path.join(self.data_dir, "chunks", digest)

    @staticmethod
    def _hash(data: bytes) -> str:
        return hashlib.sha1(data).hexdigest()

    # ---- 内部：put/delete/gc 的状态变更（日志重放也走这里） ----

    def _apply_put(self, name: str, data: bytes):
        if name in self.objects:  # 同名覆盖：先减旧引用
            self._release(self.objects[name])
        digests = []
        for offset in range(0, len(data), self.chunk_size):
            block = bytes(data[offset:offset + self.chunk_size])
            digest = self._hash(block)
            if digest in self.refcounts:  # 命中已有块，全局只存一份
                self.dedup_hits += 1
                self.refcounts[digest] += 1
            else:
                self.chunks[digest] = block
                self.refcounts[digest] = 1
                self.stored_bytes += len(block)
                if self.data_dir:
                    with open(self._chunk_path(digest), "wb") as fh:
                        fh.write(block)
            digests.append(digest)
        self.objects[name] = digests

    def _release(self, digests):
        for digest in digests:
            self.refcounts[digest] -= 1
            self._gc_candidates.add(digest)

    def _apply_delete(self, name: str) -> bool:
        if name not in self.objects:
            return False
        self._release(self.objects.pop(name))
        return True

    def _apply_gc(self):
        collected = [d for d in self._gc_candidates if self.refcounts.get(d, 0) == 0]
        collected_texts = sorted(self._text(d) for d in collected)
        for digest in collected:
            self.stored_bytes -= len(self.chunks.pop(digest))
            self.refcounts.pop(digest, None)
            if self.data_dir and os.path.exists(self._chunk_path(digest)):
                os.remove(self._chunk_path(digest))
        survivors = list(self._gc_candidates - set(collected))
        self._gc_candidates.clear()
        return collected_texts, survivors

    # ---- 对外接口 ----

    def put(self, name: str, data: bytes) -> dict:
        data = bytes(data)
        self._apply_put(name, data)
        self._log({"op": "put", "name": name, "data": data.hex()})
        return {"stored": len(data), "chunks": len(self.objects[name])}

    def get(self, name: str):
        digests = self.objects.get(name)
        if digests is None:
            return None
        parts = []
        for digest in digests:
            block = self.chunks.get(digest)
            if block is None and self.data_dir:  # 冷启动：从块文件读
                with open(self._chunk_path(digest), "rb") as fh:
                    block = fh.read()
                self.chunks[digest] = block
            parts.append(block)
        return b"".join(parts)

    def delete(self, name: str) -> dict:
        existed = self._apply_delete(name)
        if existed:
            self._log({"op": "delete", "name": name})
        return {"deleted": existed, "collected": 0}

    def gc(self) -> dict:
        collected_texts, survivors = self._apply_gc()
        self._log({"op": "gc"})
        return {
            "collected": len(collected_texts),
            "remaining": len(self.chunks),
            "collected_chunks": collected_texts,
            "refcounts": sorted([self._text(d), self.refcounts[d]] for d in survivors),
        }

    def _text(self, digest) -> str:
        return self.chunks[digest].decode("utf-8", "replace")

    def recover(self) -> dict:
        """重放日志重建状态；文件模式下尾部半条记录忽略。"""
        records = self._read_records()
        self.objects, self.chunks, self.refcounts = {}, {}, {}
        self.stored_bytes = 0
        self.dedup_hits = 0
        self._gc_candidates = set()
        for record in records:
            op = record.get("op")
            if op == "put":
                self._apply_put(record["name"], bytes.fromhex(record["data"]))
            elif op == "delete":
                self._apply_delete(record["name"])
            elif op == "gc":
                self._apply_gc()
        return {"objects": len(self.objects), "chunks": len(self.chunks)}

    def _read_records(self):
        if not self.data_dir:
            return list(self.wal)
        if not os.path.exists(self._wal_path()):
            return []
        with open(self._wal_path(), "rb") as fh:
            lines = fh.read().split(b"\n")
        if lines and lines[-1] == b"":
            lines.pop()
        records = []
        for line in lines:
            try:
                records.append(json.loads(line))
            except ValueError:
                break  # 尾部半条记录，忽略
        return records

    def stats(self) -> dict:
        return {"objects": len(self.objects), "chunks": len(self.chunks),
                "stored_bytes": self.stored_bytes, "dedup_hits": self.dedup_hits,
                "chunk_size": self.chunk_size}
