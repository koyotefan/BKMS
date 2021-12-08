# coding: utf-8

import butil
import os
import sys
import time
import signal

class Dummy:
    def write(self, s): pass

class ProcState(object):
    def __init__(self):
        self.last_update_time = 0

    def run(self, db, name):
        now = time.time()
        if now > self.last_update_time + 5:
            self.update(db, name, 'RUN')
            self.last_update_time = now

    def down(self, db, name):
        self.update(db, name, 'DONW')

    def update(self, db, name, state):
        sql = "UPDATE T_PROC_BOARD SET update_time=NOW(), state='%s'" % state
        sql +=" WHERE proc_name='%s'" % name 

        if db.query(sql) > 0:
            return

        sql = "INSERT INTO T_PROC_BOARD (proc_name, update_time, state) "
        sql+= " VALUES ('%s', NOW(), '%s')" % (name, state)
        db.query(sql)

    '''
    def reg_signal(self, handler):

        signal.signal(signal.SIGABRT, handler)
        signal.signal(signal.SIGBUS,  handler)
        signal.signal(signal.SIGILL,  handler)
        signal.signal(signal.SIGSEGV, handler)
        signal.signal(signal.SIGTERM, handler)
    '''

    @staticmethod
    def make_daemon():
        if os.fork():
            os._exit(0)

        os.setpgrp()
        os.umask(0)

        sys.stdin.close()
        sys.stdout = Dummy()
        sys.stderr = Dummy()

        
