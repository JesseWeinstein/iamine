#!/usr/bin/env python3
import json
import sys
import os
import logging
import collections
import urllib.parse

logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)
HASHES = ['md5', 'sha1']
KINDS = ['unavailable', 'public', 'private']


def normalize_newlines(s):
    return s.replace('%0D%0A', '[newline]').replace('%0A', '[newline]').replace('%0D', '[newline]')


class Actions:
    def __init__(self, results_dir, group_name, piece):
        self.results_dir = results_dir
        self.group_name = group_name
        self.piece = piece

    def fh_filename(self, kind, hash):
        filename = '_'.join(['file_hashes', kind, hash, self.group_name, self.piece+'.tsv'])
        return os.path.join(self.results_dir, filename)

    def census_data_filename(self, kind):
        filename = '_'.join(['census_data', kind, self.group_name, self.piece+'.json'])
        return os.path.join(self.results_dir, filename)

    def action_show_missing(self, piece_prefix):
        logger = logging.getLogger('show_missing')
        expected = set(open(piece_prefix + self.piece).readlines())
        for kind in KINDS:
            filename = self.census_data_filename(kind)
            logger.info(os.path.basename(filename))
            tmp = set()
            for x in open(filename):
                tmp.add(json.loads(x)['id']+'\n')
                if len(tmp) > 1000:
                    sys.stderr.write('.')
                    sys.stderr.flush()
                    expected.difference_update(tmp)
                    tmp.clear()
            sys.stderr.write('\n')
            expected.difference_update(tmp)
            logger.info('Remaining: ' + str(len(expected)))
        print(''.join(sorted(expected)))

    def action_copy_hashes(self):
        logger = logging.getLogger('copy_hashes')
        for kind in KINDS:
            cd_fn = self.census_data_filename(kind)
            logger.info(os.path.basename(cd_fn))
            cd = open(cd_fn)
            fhs = {hash: open(self.fh_filename(kind, hash)) for hash in HASHES}
            hlines = {hash: f.readline().split('\t') for (hash, f) in fhs.items()}
            out = open(cd_fn+'.new', 'w')
            n = 0
            while True:
                line = cd.readline()
                if not line:
                    break
                itm = json.loads(line, strict=False, object_pairs_hook=collections.OrderedDict)
                for (h, f) in fhs.items():
                    itmIdx = 0
                    while True:
                        if hlines[h][0] != itm['id']:
                            break

                        fitm = itm['files'][itmIdx]
                        itmIdx += 1
                        fname = urllib.parse.quote(fitm['name'])
                        if fname != hlines[h][1]:
                            logger.error("File out of order! ({0} != {1})".format(
                                fname, hlines[h][1]))
                            return
                        fitm[h] = hlines[h][2][:-1]
                        hlines[h] = f.readline().split('\t')
                        n += 1
                        if (n % 10000) == 0:
                            sys.stderr.write('.')
                            sys.stderr.flush()
                        if (n % 1000000) == 0:
                            sys.stderr.write('\n')
                            sys.stderr.flush()
                json.dump(itm, out, sort_keys=False, separators=(',', ':'))
                out.write('\n')
            sys.stderr.write('\n')
            cd.close()
            for f in fhs.values():
                f.close()
            out.close()

    def action_fix_filename_newlines(self):
        logger = logging.getLogger('fix_filename_newlines')
        for kind in KINDS:
            cd_fn = self.census_data_filename(kind)
            logger.info(os.path.basename(cd_fn))
            cd = open(cd_fn)
            fhs = {hash: open(self.fh_filename(kind, hash)) for hash in HASHES}
            hlines = {hash: f.readline().split('\t') for (hash, f) in fhs.items()}
            outs = {hash: open(self.fh_filename(kind, hash)+'.new', 'w') for hash in HASHES}
            n = 0
            while True:
                line = cd.readline()
                if not line:
                    break
                try:
                    itm = json.loads(line, strict=False, object_pairs_hook=collections.OrderedDict)
                except KeyboardInterrupt:
                    logger.error("Item took too long: " + line[:100])
                    sys.exit(1)

                for (h, f) in fhs.items():
                    itmIdx = 0
                    while True:
                        if hlines[h][0] != itm['id']:
                            break

                        fitm = itm['files'][itmIdx]
                        itmIdx += 1
                        fname = urllib.parse.quote(fitm['name'])
                        if fname != hlines[h][1]:
                            if normalize_newlines(fname) == normalize_newlines(hlines[h][1]):
                                logger.info("Fixing {0}".format(fname))
                                hlines[h][1] = fname
                            else:
                                logger.error("File out of order! ({0} != {1})".format(
                                   fname, hlines[h][1]))
                                sys.exit(1)
                        outs[h].write('\t'.join(hlines[h]))
                        hlines[h] = f.readline().split('\t')
                        n += 1
                        if (n % 10000) == 0:
                            sys.stderr.write('.')
                            sys.stderr.flush()
                        if (n % 1000000) == 0:
                            sys.stderr.write('\n')
                            sys.stderr.flush()
            sys.stderr.write('\n')
            cd.close()
            for h in fhs:
                fhs[h].close()
                outs[h].close()

    def action_urlencode_hash_filenames(self):
        logger = logging.getLogger('urlencode_hash_filenames')
        for kind in KINDS:
            for hash in HASHES:
                idx = (34 if hash == 'md5' else 42)
                filename = self.fh_filename(kind, hash)
                f = open(filename, 'rb')
                out = open(filename+'.new', 'w')
                whole = ""
                logger.info(filename)

                def write():
                    id, rest = whole.split('\t', 1)
                    out.write('\t'.join([id, urllib.parse.quote(rest[:-idx]), rest[-idx+1:]]))

                n = 0
                for l in f:
                    n += 1
                    if (n % 10000) == 0:
                        sys.stderr.write('.')
                        sys.stderr.flush()
                    if (n % 1000000) == 0:
                        sys.stderr.write('\n')
                        sys.stderr.flush()
                    if len(whole) >= idx and whole[-idx] == '\t' and whole[-idx+1:-1].isalnum():
                        write()
                        whole = ''
                    whole += l
                sys.stderr.write('\n')
                write()
                f.close()
                out.close()


if __name__ == '__main__':
    getattr(Actions(*sys.argv[2:5]), 'action_'+sys.argv[1])(*sys.argv[5:])
