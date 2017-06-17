#!/usr/bin/env python3
import os.path
import asyncio
import json

TOPLEVEL_KEYS = ['is_dark', 'nodownload']
FILE_KEYS = ["name", "format"]
METADATA_KEYS = ["collection", "publicdate", "noindex"]
HASH_KEYS = ["md5", "sha1"]


def make_callback(output_file_dir, timestamp):

    def open_file(prefix, suffix):
        return open(os.path.join(output_file_dir, prefix+timestamp+suffix), 'w')

    main_file = open_file("census_data_", ".json")
    hash_files = {k: open_file("file_hashes_"+k+"_", ".tsv") for k in HASH_KEYS}

    @asyncio.coroutine
    def callback(resp):
        j = yield from resp.json()
        id = resp.url.split('/')[4]
        resp.close()

        out = {"id": id}
        if 'dir' not in j:
            out["no_dir"] = "true"

        def copy_over(key, source):
            if key in source:
                out[key] = source[key]

        for name in TOPLEVEL_KEYS:
            copy_over(name, j)

        m = j.get("metadata", {})
        files = []
        ts = 0
        some_private = False
        hash_outs = {k: [] for k in HASH_KEYS}
        for x in j.get("files", []):
            if x["source"] != "derivative" and x["name"] != id + "_files.xml":
                file_info = {n: x.get(n, "") for n in FILE_KEYS}
                file_info['size'] = int(x.get("size", 0))
                if 'private' in x:
                    some_private = True
                    file_info['private'] = x['private']
                files.append(file_info)
                ts += file_info['size']
                for k in HASH_KEYS:
                    hash_outs[k].append('\t'.join([id, x['name'], x[k]]))

        if files:
            out["files"] = files
            out["total_size"] = ts

        if some_private:
            out['some_private'] = 'true'

        for name in METADATA_KEYS + HASH_KEYS:
            copy_over(name, m)

        m_id = m.get("identifier")
        if m_id and m_id != id:
            out["metadata_identifier"] = m_id

        if 'dir' in j:
            dirs = j['dir'].split("/")
            if len(dirs) < 4 or dirs[3] != id:
                out['dir'] = j['dir']

        out_json = json.dumps(out, sort_keys=True, separators=(',', ':'))
        yield from output(out_json, main_file)

        if files:
            for k in HASH_KEYS:
                yield from output("\n".join(hash_outs[k]), hash_files[k])

    @asyncio.coroutine
    def output(out, output_file):
        output_file.write(out+"\n")

    def cleanup():
        main_file.close()
        for k in HASH_KEYS:
            hash_files[k].close()

    callback.cleanup = cleanup

    return callback
