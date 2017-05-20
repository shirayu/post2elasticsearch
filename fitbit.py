#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Post data to ElasticSearch
'''

import argparse
import codecs
import sys
import json
import os

sys.path.append(os.path.dirname(__file__))
# pylint: disable=wrong-import-position
import common
# pylint: enable=wrong-import-position

INDEX_NAME = 'fitbit'
MAPPING = {
    "properties": {
        "date": {"type": "date", "format": "YYYY-MM-dd"}
    }
}


def operation(address, index_name, inf):
    '''
    Main
    '''
    for line in inf:
        data = json.loads(line)
        date = data['elevation']['activities-elevation'][0]['dateTime']

        pdata = {
            'date': date,
        }

        for key in data:
            if key == 'sleep':
                pdata.update(data[key]['summary'])
            else:
                val = data[key]['activities-%s' % key][0]['value']
                if key == 'heart':
                    if 'restingHeartRate' in val:
                        pdata['restingHeartRate'] = val['restingHeartRate']
                elif key == 'distance':
                    pdata[key] = float(val)
                else:
                    pdata[key] = int(val)

        common.post_data(address, index_name, "date", pdata, date)


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
    oparser.add_argument("--iname", dest="iname", default=INDEX_NAME)
    opts = oparser.parse_args()

    if opts.input == "-":
        inf = sys.stdin
    else:
        inf = codecs.open(opts.input, "r", "utf8")

    if opts.init:
        common.initialization(opts.address, opts.iname, MAPPING)
    else:
        operation(opts.address, opts.iname, inf)


if __name__ == '__main__':
    main()
