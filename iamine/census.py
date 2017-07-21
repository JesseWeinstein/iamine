#!/usr/bin/env python3
import os.path
import asyncio
import json
import logging
import collections

AVAIL_CATS = ["public", "private", "unavailable"]
HASH_KEYS = ["md5", "sha1"]
TOPLEVEL_KEYS = ['is_dark', 'nodownload']
METADATA_KEYS = ["collection", "publicdate", "noindex"]
FILE_KEYS = ["name", "format"]

logger = logging.getLogger('census')
logger.setLevel(logging.DEBUG)


def non_strict_json_loads(x):
    return json.loads(x, strict=False)


def make_callback(output_file_dir, timestamp):

    def open_file(prefix, suffix):
        return open(os.path.join(output_file_dir, prefix+timestamp+suffix), 'w')

    main_files = {a: open_file("census_data_"+a+"_", ".json") for a in AVAIL_CATS}
    hash_files = {a: {k: open_file("file_hashes_"+a+"_"+k+"_", ".tsv") for k in HASH_KEYS}
                  for a in AVAIL_CATS}

    @asyncio.coroutine
    def callback(resp):
        id = resp.url.split('/')[4]
        logger.info(id)
        if resp.status != 200:
            logger.warning('HTTP status for ' + id + ' was ' + str(resp.status) + '. Giving up.')
            resp.close()
            return
        j = yield from resp.json(encoding="utf8", loads=non_strict_json_loads)
        resp.close()

        out = collections.OrderedDict({"id": id})

        if 'dir' not in j:
            out["no_dir"] = True
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

        copy_multiple_over(METADATA_KEYS, m)

        m_id = m.get("identifier")
        if m_id and m_id != id:
            out["metadata_identifier"] = m_id

        hash_outs = {k: [] for k in HASH_KEYS}

        @asyncio.coroutine
        def do_files():
            logger.info(id)
            files = []
            ts = 0
            some_private = False
            for x in j.get("files", []):
                if x["source"] == "derivative" or x["name"] == id + "_files.xml":
                    continue
                files.append({n: x.get(n, "") for n in FILE_KEYS + HASH_KEYS})
                if copy_over('private', x, files[-1]):
                    some_private = True
                files[-1]['size'] = int(x.get("size", 0))
                ts += files[-1]['size']
                for k in HASH_KEYS:
                    if k not in x:
                        logger.warning('File "{0[name]}" in {1} has no {2} hash!'.format(x, id, k))
                    elif type(x[k]) != str:
                        logger.warning('File "{0[name]}" in {1} has multiple {2} hashes: {3}'
                                       .format(x, id, k, x[k]))
                    else:
                        hash_outs[k].append('\t'.join([id, x['name'], x[k]]))

            if some_private:
                out['some_private'] = 'true'

            if files:
                out["total_size"] = ts
                out["files"] = files

        yield from do_files()

        if 'files' not in out or \
           'is_dark' in out or \
           'no_dir' in out or \
           'noindex' in out:
            avail = "unavailable"
        elif 'some_private' in out:
            avail = "private"
        else:
            avail = "public"

        out_json = json.dumps(out, sort_keys=False, separators=(',', ':'))

        @asyncio.coroutine
        def do_output(out, output_file):
            logger.info(id + ' -> ' + output_file.name)
            output_file.write(out+"\n")

        yield from do_output(out_json, main_files[avail])

        if 'files' in out:
            for k in HASH_KEYS:
                yield from do_output("\n".join(hash_outs[k]), hash_files[avail][k])
        logger.info(id + " DONE!")

    def cleanup():
        for a in AVAIL_CATS:
            main_files[a].close()
            for k in HASH_KEYS:
                hash_files[a][k].close()

    callback.cleanup = cleanup

    return callback
