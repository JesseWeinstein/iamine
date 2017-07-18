#!/usr/bin/env python3
import json
import sys
import os
import logging

logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)


def show_missing(piece_prefix, results_dir, group_name, piece):
    logger = logging.getLogger('show_missing')
    expected = set(open(piece_prefix+piece).readlines())
    for kind in ['unavailable', 'public', 'private']:
        filename = 'census_data_'+kind+'_'+group_name+'_'+piece+'.json'
        logger.info(filename)
        tmp = set()
        for x in open(os.path.join(results_dir, filename)):
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


if __name__ == '__main__':
    if sys.argv[1] == 'show_missing':
        show_missing(*sys.argv[2:])
