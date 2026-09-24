import json
import threading
import unittest
import urllib.error
import urllib.request

from chunkstore import ChunkStore
from server import serve

class TestChunkStore(unittest.TestCase):
    def test_put_get_roundtrip(self):
        store = ChunkStore()
        store.put("a", b"abcd")
        self.assertEqual(store.get("a"), b"abcd")

    def test_missing_object(self):
        self.assertIsNone(ChunkStore().get("nope"))

    def test_delete_reports(self):
        store = ChunkStore()
        store.put("a", b"abcd")
        self.assertTrue(store.delete("a")["deleted"])

    def test_stats_shape(self):
        self.assertIn("stored_bytes", ChunkStore().stats())

    def test_http_put_get(self):
        server = serve(0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = "http://127.0.0.1:%d" % server.server_port
        urllib.request.urlopen(base + "/put", data=b'{"name": "a", "data": "abcd"}', timeout=5).read()
        with urllib.request.urlopen(base + "/get", data=b'{"name": "a"}', timeout=5) as response:
            self.assertEqual(json.loads(response.read())["data"], "abcd")
        server.shutdown()
