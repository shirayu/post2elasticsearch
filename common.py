#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
common functions
'''

import sys
import json
import urllib.request


def initialization(address, index_name, mapping):
    '''
    Initialize index
    '''
    base_url = address + '/' + index_name
    # Delete
    try:
        req = urllib.request.Request(url=base_url, method='DELETE')
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as err:
        sys.stderr.write("(delete) %s\n" % err)

    # Put
    req2 = urllib.request.Request(url=base_url, method='PUT')
    urllib.request.urlopen(req2)

    encoded_post_data = json.dumps(mapping).encode(encoding='ascii')
    req3 = urllib.request.Request(url=base_url + '/log/_mapping',
                                  data=encoded_post_data,
                                  method='PUT')
    res3 = urllib.request.urlopen(req3)
    if res3.status == 200:
        sys.stderr.write("OK. Initialized.\n")


def post_data(address, index_name, type_name, pdata, did=None):
    '''
    Post data
    '''
    base_url = address + "/" + index_name + "/" + type_name
    if did is not None:
        base_url += "/" + did

    encoded_post_data = json.dumps(pdata).encode(encoding='ascii')
    req = urllib.request.Request(url=base_url,
                                 data=encoded_post_data,
                                 method='POST')
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as err:
        sys.stderr.write("%s\n%s\n" % (err.reason, err.read().decode('utf8')))
