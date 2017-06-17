#!/usr/bin/env python3
import os.path
import asyncio
import json
import logging

TOPLEVEL_KEYS = ['is_dark', 'nodownload']
FILE_KEYS = ["name", "format"]
METADATA_KEYS = ["collection", "publicdate", "noindex"]
HASH_KEYS = ["md5", "sha1"]

logger = logging.getLogger('census')
logger.setLevel(logging.DEBUG)


def make_callback(output_file_dir, timestamp):

    def open_file(prefix, suffix):
        return open(os.path.join(output_file_dir, prefix+timestamp+suffix), 'w')

    main_file = open_file("census_data_", ".json")
    hash_files = {k: open_file("file_hashes_"+k+"_", ".tsv") for k in HASH_KEYS}

    @asyncio.coroutine
    def callback(resp):
        id = resp.url.split('/')[4]
        logger.info('Got metadata for ' + id)
        j = yield from resp.json()
        resp.close()

        out = {"id": id}

        if 'dir' not in j:
            out["no_dir"] = "true"
        else:
            dirs = j['dir'].split("/")
            if len(dirs) < 4 or dirs[3] != id:
                out['dir'] = j['dir']

        def copy_over(key, source, dest):
            if key in source:
                dest[key] = source[key]
                return True

        def copy_multiple_over(keys, source):
            for key in keys:
                copy_over(key, source, out)

        copy_multiple_over(TOPLEVEL_KEYS, j)

        m = j.get("metadata", {})

        copy_multiple_over(METADATA_KEYS + HASH_KEYS, m)

        m_id = m.get("identifier")
        if m_id and m_id != id:
            out["metadata_identifier"] = m_id

        hash_outs = {k: [] for k in HASH_KEYS}

        @asyncio.coroutine
        def do_files():
            files = []
            ts = 0
            some_private = False
            for x in j.get("files", []):
                if x["source"] == "derivative" or x["name"] == id + "_files.xml":
                    continue
                files.append({n: x.get(n, "") for n in FILE_KEYS})
                if copy_over('private', x, files[-1]):
                    some_private = True
                files[-1]['size'] = int(x.get("size", 0))
                ts += files[-1]['size']
                for k in HASH_KEYS:
                    hash_outs[k].append('\t'.join([id, x['name'], x[k]]))

            if files:
                out["files"] = files
                out["total_size"] = ts

            if some_private:
                out['some_private'] = 'true'

        yield from do_files()

        out_json = json.dumps(out, sort_keys=True, separators=(',', ':'))
        yield from output(out_json, main_file)

        if 'files' in out:
            for k in HASH_KEYS:
                yield from output("\n".join(hash_outs[k]), hash_files[k])
        logger.info("Finished "+id)

    @asyncio.coroutine
    def output(out, output_file):
        output_file.write(out+"\n")

    def cleanup():
        main_file.close()
        for k in HASH_KEYS:
            hash_files[k].close()

    callback.cleanup = cleanup

    return callback
