"""check_http.py：起服务、按脚本走一圈，打印验收面。"""
import json
import sys
import threading
import urllib.error
import urllib.request

from server import serve


def call(method, url, body=None):
    request = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": (text or "")[:60]}


def main() -> int:
    spec = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "sample/chunks.json", encoding="utf-8"))
    server = serve(0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:%d" % server.server_port
    for item in spec["puts"]:
        call("POST", base + "/put", json.dumps(item).encode())
    after_put = parse(call("GET", base + "/stats")[1])
    deleted = parse(call("POST", base + "/delete", json.dumps({"name": spec["delete"]}).encode())[1])
    collected = parse(call("POST", base + "/gc", b"{}")[1])
    recovered = parse(call("POST", base + "/recover", b"{}")[1])
    read_back = parse(call("POST", base + "/get", json.dumps({"name": spec["delete"]}).encode())[1])
    keeps = parse(call("POST", base + "/get", json.dumps({"name": spec["keep"]}).encode())[1])
    print("唯一块数 =", after_put.get("chunks"))
    print("去重命中数 =", after_put.get("dedup_hits"))
    print("落盘字节 =", after_put.get("stored_bytes"))
    print("删除对象 =", deleted.get("deleted"))
    print("GC 回收块数 =", collected.get("collected"))
    print("GC 后剩余块数 =", collected.get("remaining"))
    print("共享块引用计数 =", collected.get("refcounts"))
    print("重启恢复后对象数 =", recovered.get("objects"))
    print("恢复后块数 =", recovered.get("chunks"))
    print("被删对象是否还能读到 =", read_back.get("data") is not None)
    print("共享块所属对象仍可读 =", keeps.get("data"))
    server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
