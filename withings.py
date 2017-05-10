#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Post data to ElasticSearch
'''

import argparse
import codecs
from datetime import datetime
import sys
import json
import os

sys.path.append(os.path.dirname(__file__))
# pylint: disable=wrong-import-position
import common
# pylint: enable=wrong-import-position

MAPPING = {
    "properties": {
        "date": {"type": "date", "format": "YYYY-MM-dd HH:mm:ss"}
    }
}


def operation(address, index_name, inf, height=None):
    '''
    Main
    '''
    for line in inf:
        data = json.loads(line)
        if data['weight'] is None:
            continue

        timestamp = data['raw']['date']
        utc = str(datetime.utcfromtimestamp(timestamp))
        pdata = {
            'date': utc,
            'weight': data['weight'],
            'fat_ratio': data['fat_ratio'],
            'fat_mass_weight': data['fat_mass_weight'],
        }
        if (height is not None) and (height != 0):
            bmi = (data['weight'] / height * 100 / height * 100)
            pdata['body_mass_index'] = bmi
        common.post_data(address, index_name, "date", pdata, str(timestamp))


def main():
    '''
    Parse arguments
    '''
    oparser = argparse.ArgumentParser()
    oparser.add_argument("-i", "--input", dest="input", default="-")
    oparser.add_argument("-a", "--address", dest="address", default="http://localhost:9200")
    oparser.add_argument("--init", dest="init", default=False, action="store_true")
    oparser.add_argument("--height", dest="height", default=None,
                         type=float, help="Your height (centimeter)")
    oparser.add_argument("--iname", dest="iname", default="withings")
    opts = oparser.parse_args()

    if opts.input == "-":
        inf = sys.stdin
    else:
        inf = codecs.open(opts.input, "r", "utf8")

    if opts.init:
        common.initialization(opts.address, opts.iname, MAPPING)
    else:
        operation(opts.address, opts.iname, inf, opts.height)


if __name__ == '__main__':
    main()
