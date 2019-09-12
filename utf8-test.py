#!/usr/bin/env python

from datetime import datetime
from optparse import OptionParser
import mysql.connector
import os
import sys
import time
import traceback

cwd = os.path.dirname(os.path.realpath(__file__))

meta_host = '127.0.0.1'
meta_port = 33306
meta_user = 'msandbox'
meta_pass = 'msandbox'
meta_db = 'utf8db'

utf8mb4s = [
    'トラストエレ＿デイリーレポート',
    'Välkkynen Iltalehti osiot ',
    'gångna månaden',
    'Użycie danych - kampanie',
    'Seif månadsrapport',
    'lorem imsum täglich '
]

def date(unixtime, format = '%m/%d/%Y %H:%M:%S'):
    d = datetime.fromtimestamp(unixtime)
    return d.strftime(format)

def _out(tag, *msgs):
    s = ''

    if not msgs:
        return

    for msg in msgs:
        s += str(msg)

    print "[%s] %s: %s" % (date(time.time()), tag, s)

def _say(*msgs):
    _out('INFO', *msgs)

def _warn(*msgs):
    _out('WARN', *msgs)

def _error(*msgs):
    _out('ERROR', *msgs)

def _die(*msgs):
    _out('FATAL', *msgs)
    raise Exception(str(msgs))

class RunUtf8Test(object):
    def __init__(self):
        parser = OptionParser('Usage: %prog [options]')
        parser.add_option('-c', '--charset', dest='charset', type='string',
            help='Character set to use for this test', default='utf8mb4')

        (self.opts, args) = parser.parse_args()

    def run(self):
        meta_con = mysql.connector.connect(
            user=meta_user, password=meta_pass, host=meta_host,
            database=meta_db, port=meta_port, charset=self.opts.charset, 
            allow_local_infile=True, autocommit=True, use_pure=True)

        

        meta_con.close()
        return 0

if __name__ == "__main__":
    try:
        tester = RunUtf8Test()
        sys.exit(tester.run())
    except Exception, e:
        traceback.print_exc()
        sys.exit(1)
        

