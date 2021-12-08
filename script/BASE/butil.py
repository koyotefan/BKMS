# coding: utf-8

import os
import socket
import ConfigParser
import traceback

class Error(Exception): pass
class ProxyDBErr(Error): pass
class WorkError(Error): pass

_MACRO = {
    'LOG_PATH':os.getenv('HOME') + os.sep + 'log',
    'CONFIG_FILE':os.getenv('HOME') + os.sep + 'app/conf/bkms.cfg',
    'DATA_PATH':'/data1',
    'TERM_TIME':'06',
    'RUN_TIME':'18',
}

# CODE : 'REASON'

_ALARM = {
    0:'NOERR',
    400:'DB_ERR',
    401:'DB_SELECT_ERR',
    501:'WORK_TIMEOUT',
    502:'WORK_PARAMETER_ERR',
    503:'WORK_CREATE_ERR',
}

class   Alarm(object):
    def __init__(self):
        self.code   = 0
        self.reason = ''

        self.alarm_list = []

    def occur(self, code, reason):

        if not self.code:
            self.code = code
            self.reason = reason

        self.alarm_list.append((code, reason))

    def was_occur(self):
        return self.code != 0

    def result(self):
        if self.code in [0]:
            return 'SUCCESS'

        if self.code in [300, 301, 302]:
            return 'ERROR'

        return 'FAIL'

    def state(self):

        if self.code in [300,301,302]:
            return 'RETRY'

        return 'TERM'

    def get_history(self):
        return self.alarm_list

class DbProxy(object):
    def __init__(self):
        self.sock = None
        self.sep  = '\0'
        self.L    = None

    def open(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((ip, int(port)))
        self.L.log(0, 'INF| DbProxy:: connect success')

    def set_log(self, L):
        self.L = L

    def close(self):
        try:
            self.sock.close()
            self.sock = None
            self.L.log(0, 'WRN| DbProxy:: sock close')
        except:
            pass

    def read_until(self, sep):
        index = 0
        data = ''

        while True:
            char = self.sock.recv(1)

            if char == '':
                raise RuntimeError('socket recv fail')

            if char == sep:
                break

            data += char

        self.L.log(3, 'INF| DbProxy:: recv [%s]' % data)
        return data

    def query(self, sql):
        stmt = sql
        stmt += self.sep

        self.L.log(2, 'INF| DbProxy:: query [%s]' % sql)
        self.sock.sendall(stmt)
        ret = self.read_until(self.sep)

        # expecte ret_value\t
        ret = ret.strip()
        self.L.log(2, 'INF| DbProxy:: query ret [%s]' % ret)
        return int(ret)

    def fetch(self):
        self.sock.sendall(self.sep)
        ret = self.read_until(self.sep)

        # expecte ret_value\tv1\tv2\tv3\t...
        ret = ret.split('\t')
        ret.pop()
        self.L.log(3, 'INF| DbProxy:: fetch [%s]' % ret)

        if int(ret.pop(0)) < 0:
            raise ProxyDBErr('fetch error')

        self.L.log(2, 'INF| DbProxy:: fetch [%s]' % ret)
        return ret

    def fetch_all(self):
        ret = []
        while True:
            r = self.fetch()
            if not r:
                break
            ret.append(r)

        return ret


