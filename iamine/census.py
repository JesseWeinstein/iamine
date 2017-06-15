#!/usr/bin/env python3
import asyncio
import json

FILE_KEYS = ["name", "format", "md5", "sha1"]
METADATA_KEYS = ["collection", "publicdate", "noindex"]


@asyncio.coroutine
def callback(resp):
    j = yield from resp.json()
    id = resp.url.split('/')[4]
    resp.close()
    out = {"id": id}
    if 'dir' not in j:
        out["no_dir"] = "true"

    if 'files' not in j:
        out["is_dark"] = j.get("is_dark")

    m = j.get("metadata", {})
    files = []
    ts = 0
    some_private = False
    for x in j.get("files", []):
        if x["source"] != "derivative" and x["name"] != id + "_files.xml":
            file_info = {n: x.get(n, "") for n in FILE_KEYS}
            file_info['size'] = int(x.get("size", 0))
            if 'private' in x:
                some_private = True
                file_info['private'] = x['private']
            files.append(file_info)
            ts += file_info['size']

    if files:
        out["files"] = files
        out["total_size"] = ts

    if some_private:
        out['some_private'] = 'true'

    if 'nodownload' in j:
        out['nodownload'] = j['nodownload']

    for name in METADATA_KEYS:
        if name in m:
            out[name] = m[name]

    m_id = m.get("identifier")
    if m_id and m_id != id:
        out["metadata_identifier"] = m_id

    if 'dir' in j:
        dirs = j['dir'].split("/")
        if len(dirs) < 4 or dirs[3] != id:
            out['dir'] = j['dir']

    print(json.dumps(out))
