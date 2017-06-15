#!/usr/bin/env python3
import os.path
import asyncio
import json

FILE_KEYS = ["name", "format", "md5", "sha1"]
METADATA_KEYS = ["collection", "publicdate", "noindex"]


def make_callback(output_file_dir, timestamp):

    main_file = open(os.path.join(output_file_dir, "census_data_"+timestamp+".json"), 'w')
    md5_file = open(os.path.join(output_file_dir, "file_hashes_md5_"+timestamp+".tsv"), 'w')

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
        md5out = []
        for x in j.get("files", []):
            if x["source"] != "derivative" and x["name"] != id + "_files.xml":
                file_info = {n: x.get(n, "") for n in FILE_KEYS}
                file_info['size'] = int(x.get("size", 0))
                if 'private' in x:
                    some_private = True
                    file_info['private'] = x['private']
                files.append(file_info)
                ts += file_info['size']
                md5out.append('\t'.join([id, x['name'], x['md5']]))

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

        yield from output_json(out, main_file)
        yield from output_tsv("\n".join(md5out), md5_file)

    @asyncio.coroutine
    def output_json(out, output_file):
        output_file.write(json.dumps(out, sort_keys=True, separators=(',', ':'))+"\n")

    @asyncio.coroutine
    def output_tsv(out, output_file):
        output_file.write(out)

    def cleanup():
        main_file.close()
        md5_file.close()

    callback.cleanup = cleanup

    return callback
