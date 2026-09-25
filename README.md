# chunkstore

纯 Python 标准库的本机服务。

## 功能

- 分块与内容寻址：`put` 按 `chunk_size` 切块，相同内容的块全局只存一份（SHA-1 寻址），命中计入 `dedup_hits`；`get` 按块拼回原对象。
- 引用计数：每个块记录引用数，同名覆盖先减旧引用；`stats()` 返回对象数、块数、落盘字节、去重命中、块大小。
- 回收：`delete` 只减引用；`gc()` 回收引用为 0 的块，返回 `collected` / `remaining` / `collected_chunks` / `refcounts`，共享块不会被回收。
- 持久化：写、删除、GC 追加到 `data/wal.log`，块内容落在 `data/chunks/`；`recover()` 重放日志重建对象与引用计数，尾部半条记录自动忽略。

## 起服务

    python3 server.py 8000

浏览器打开 http://127.0.0.1:8000/ 看结果。

## 测试

    python3 -m unittest discover -s tests -v

## 验收自检

    python3 check_http.py
