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
import urllib.request

INDEX_NAME = 'withings'
MAPPING = {
    "properties": {
        "date": {"type": "date", "format": "YYYY-MM-dd HH:mm:ss"}
    }
}


def initialization(address):
    '''
    Initialize index
    '''
    base_url = address + '/' + INDEX_NAME
    # Delete
    try:
        req = urllib.request.Request(url=base_url, method='DELETE')
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as err:
        sys.stderr.write("(delete) %s\n" % err)

    # Put
    req2 = urllib.request.Request(url=base_url, method='PUT')
    urllib.request.urlopen(req2)

    encoded_post_data = json.dumps(MAPPING).encode(encoding='ascii')
    req3 = urllib.request.Request(url=base_url + '/log/_mapping',
                                  data=encoded_post_data,
                                  method='PUT')
    res3 = urllib.request.urlopen(req3)
    if res3.status == 200:
        sys.stderr.write("OK. Initialized.\n")


def post_data(address, timestamp, pdata):
    '''
    Post data
    '''
    base_url = "%s/%s/date/%d" % (address, INDEX_NAME, timestamp)

    encoded_post_data = json.dumps(pdata).encode(encoding='ascii')
    req = urllib.request.Request(url=base_url,
                                 data=encoded_post_data,
                                 method='POST')
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as err:
        sys.stderr.write("%s\n%s\n" % (err.reason, err.read().decode('utf8')))


def operation(address, inf):
    '''
    Main
    '''
    for line in inf:
        data = json.loads(line)

        timestamp = data['raw']['date']
        utc = str(datetime.utcfromtimestamp(timestamp))
        pdata = {
            'date': utc,
            'weight': data['weight'],
            'fat_ratio': data['fat_ratio'],
            'fat_mass_weight': data['fat_mass_weight'],
        }
        post_data(address, timestamp, pdata)


def main():
    '''
    Parse arguments
    '''
    oparser = argparse.ArgumentParser()
    oparser.add_argument("-i", "--input", dest="input", default="-")
    oparser.add_argument("-a", "--address", dest="address", default="http://localhost:9200")
    oparser.add_argument("--init", dest="init", default=False, action="store_true")
    opts = oparser.parse_args()

    if opts.input == "-":
        inf = sys.stdin
    else:
        inf = codecs.open(opts.input, "r", "utf8")

    if opts.init:
        initialization(opts.address)
    else:
        operation(opts.address, inf)


if __name__ == '__main__':
    main()
