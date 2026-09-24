"""server.py：本机服务（基线：put/get/delete/stats）。"""
from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from chunkstore import ChunkStore

STORE = ChunkStore()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(STORE.stats()).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.loads(self.rfile.read(length) or b"{}")
        if self.path.startswith("/put"):
            result = STORE.put(payload["name"], payload["data"].encode())
        elif self.path.startswith("/get"):
            raw = STORE.get(payload["name"])
            result = {"data": None if raw is None else raw.decode()}
        elif self.path.startswith("/delete"):
            result = STORE.delete(payload["name"])
        elif self.path.startswith("/gc"):
            result = STORE.gc()
        elif self.path.startswith("/recover"):
            result = STORE.recover()
        else:
            result = {}
        body = json.dumps(result).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def serve(port: int = 0):
    return HTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    print("listening on http://127.0.0.1:%d" % port)
    serve(port).serve_forever()
