#!/usr/bin/env python3
import asyncio
import json

FILE_KEYS = ["name", "format", "md5", "sha1"]
METADATA_KEYS = ["collection", "publicdate", "noindex"]


@asyncio.coroutine
def callback(resp):
    j = yield from resp.json()
    resp.close()
    if 'files' not in j:
        out = {"id": j.get("dir", "").split("/")[3], "is_dark": j.get("is_dark")}
    else:
        m = j.get("metadata", {})
        id = m.get("identifier")
        files = []
        ts = 0
        some_private = []
        for x in j["files"]:
            if x["source"] != "derivative" and x["name"] != id + "_files.xml":
                size = int(x.get("size", 0))
                has_private = maybe_present(x, "private")
                files.append(dict([(n, x.get(n, "")) for n in FILE_KEYS] +
                                  [("size", size)] + has_private))
                ts += size
                if has_private:
                    some_private = [("some_private", "true")]

        out = dict([("id", id), ("files", files), ("total_size", ts)] +
                   some_private + maybe_present(j, "nodownload") +
                   [(name, m[name]) for name in METADATA_KEYS if name in m])
    print(json.dumps(out))


def maybe_present(x, key):
    return ([(key, x[key])] if key in x else [])
