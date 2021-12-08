
#coding: utf-8

# commander 를 여러개 수용할 수 있어야 해요..

import threading
import time
import ConfigParser
import traceback
import sys
import os
import subprocess

import KytLog
from BASE import butil
from BASE import bproc
from WORKER import BkmsTelnet

_SERVICE_STATE = True


class Action(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.lock = None 
        self.L    = None 
        self.db   = None 
        self.key  = '' 
        self.cmd  = '' 

    def init(self, log, db, lock, key, cmd):
        self.lock = lock
        self.L   = log
        self.db  = db
        self.key = key
        self.cmd = cmd

    def _update_start(self):
        # DB Update   
        sql = "UPDATE T_CMD_REQUEST SET state='ING' WHERE id=%s" % self.key
        self.lock.acquire()
        ret = self.db.query(sql)
        self.lock.release()

        if ret <= 0:
            self.L.log(0,
                    "WRN| query fail. [%s] [ret:%s]", sql)
            return False

        return True

    def _insert_result(self, rst):
        # DB Insert
        sql = "INSERT INTO T_CMD_RESULT (id, resp_time, value_type, value) "
        sql+= " VALUES ('%s', NOW(), '', '%s')" % (self.key, rst)

        self.lock.acquire()
        ret = self.db.query(sql)
        self.lock.release()

        if ret <= 0:
            self.L.log(0,
                    "WRN| query fail. [%s] [ret:%s]", sql, ret)
            return False

        return True

    def _update_end(self, result, reason):
        # DB update
        sql = "UPDATE T_CMD_REQUEST SET state='TERM', "
        sql += " result='%s',reason='%s', resp_time=NOW() " % (result,reason)
        sql += " WHERE id=%s" % self.key

        self.lock.acquire()
        ret = self.db.query(sql)
        self.lock.release()

        if ret <= 0:
            self.L.log(0,
                    "WRN| query fail. [%s] [ret:%s]", sql, ret)
            return False

        return True

    def run(self):
        self.L.log(0, '==================================================')
        self.L.log(0, 'INF| action start [%s]' % self.cmd)

        # DB Update
        if not self._update_start():
            return


        (result, reason, value) = self.do_action(self.cmd)

        # DB Insert
        if not self._insert_result(value):
            return

        # DB Update
        if not self._update_end(result, reason):
            return 

        self.L.log(0, 'INF| end [%s] [%s]' % (self.cmd, result))

class RouteActionForSolaris(Action):
    def __init__(self):
        Action.__init__(self)

    def _rewrite_route_tbl(self, rst):
        # TBL delete
        sql = "DELETE FROM T_ROUTE_BOARD"
        self.lock.acquire()
        ret = self.db.query(sql)
        self.lock.release()

        # INSERT
        sql = "INSERT INTO T_ROUTE_BOARD "
        sql +="(update_time, destination, gateway, flags, ref, `use`, interface)"
        sql += " VALUES (NOW(), '%s', '%s', '%s', '%s', '%s', '%s')" 

        for line in rst.split('\n'):
            v_list = line.split()
            if len(v_list) != 6:
                continue

            if '--' in line or 'Destination' in line:
                continue

            r_sql = sql % (v_list[0], v_list[1], v_list[2], 
                           v_list[3], v_list[4], v_list[5]) 

            self.lock.acquire()
            ret = self.db.query(r_sql)
            self.lock.release()

            if ret <= 0:
                self.L.log(0,
                        "WRN| query fail. [%s] [ret:%s]", r_sql, ret)
                return False

        
        self.L.log(2, "INF| _rewrite_route_tbl:: success")

    def do_action(self, cmd):
        rst = ''
        result = 'SUCCESS'
        reason = ''

        try:
            p = subprocess.Popen(cmd, 
                                 shell=True, 
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)

            rst = ''
            for line in p.stdout.readlines():
                rst += line

            retval = p.wait()
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| run except %s' % err_val)
            self.L.log(0, 'ERR| run except %s' % last_err_val)

            result = 'ERROR'
            reason = last_err_val

        self.L.log(3, 'INF| ret [%s]' % rst)

        if result == 'SUCCESS':
            self._rewrite_route_tbl(rst)

        return (result, reason, rst)

class LoginAction(Action):
    def __init__(self):
        Action.__init__(self)

    def do_action(self, cmd):
        rst = ''
        result = 'SUCCESS'
        reason = ''

        cmd_list = cmd.split()

        if len(cmd_list) != 5:
            self.L.log(0, 'FAIL| do_action::login invalid arg : %s' % cmd)
            result = 'FAIL'
            reason = 'invalid cmd arguent'
            return (result, reason, '')

        try:
            itelnet = BkmsTelnet.BkmsTelnet(self.L, cmd_list[1], cmd_list[2])
            rst = itelnet.login(cmd_list[3], cmd_list[4])
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| do_action::login except : %s' % err_val) 
            self.L.log(0, 'ERR| do_action::login except : %s' % last_err_val)
           
            result = 'ERROR'
            reason = last_err_val

        self.L.log(3, 'INF| ret [%s]' % rst)

        return (result, reason, rst)


class CmdAction(Action):
    def __init__(self):
        Action.__init__(self)

    def do_action(self, cmd):
        rst = ''
        result = 'SUCCESS'
        reason = ''

        try:
            p = subprocess.Popen(cmd, 
                                 shell=True, 
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)

            rst = ''
            for line in p.stdout.readlines():
                rst += line

            retval = p.wait()
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| run except %s' % err_val)
            self.L.log(0, 'ERR| run except %s' % last_err_val)

            result = 'ERROR'
            reason = last_err_val

        self.L.log(3, 'INF| ret [%s]' % rst)

        return (result, reason, rst)

class ActionFactory(object):
    def __init__(self):
        pass

    @staticmethod
    def create_instance(cmd_type): 
        if cmd_type == 'ROUTE':
            return RouteActionForSolaris()

        if cmd_type == 'LOGIN':
            return LoginAction()

        return CmdAction()

class BCommander(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.L = KytLog.Log(3)
        self.L.init(butil._MACRO['LOG_PATH'], 'commander.log')

        self.proxy_ip   = ''
        self.proxy_port = ''

        self.proc = None

    def init(self):
        self.L.log(0, 'INF| init')

        config = ConfigParser.RawConfigParser()
        config.read(butil._MACRO['CONFIG_FILE'])
        self.proxy_ip = config.get('DB_PROXY', 'PROXY_IP')
        self.proxy_port = config.get('DB_PROXY', 'PROXY_PORT')
        config = None 

        self.db = butil.DbProxy()
        self.db.set_log(self.L)
        self.db.open(self.proxy_ip, self.proxy_port)
        self.L.log(0, 'INF| init end')

        self.proc = bproc.ProcState() 

    def _get_command(self):

        sql = "SELECT id, cmd_type, cmd FROM T_CMD_REQUEST "
        sql += " WHERE state='IDLE' ORDER BY id limit 1"

        self.lock.acquire()
        ret = self.db.query(sql)
        if ret < 0:
            self.lock.release()
            self.L.log(0, "ERR| query fail [%s] [ret:%s]" % (sql, ret))
            return (0, '', '', '')

        if ret == 0:
            self.lock.release()
            return (0, '', '', '')

        r_list = self.db.fetch()
        if not r_list:
            self.lock.release()
            self.L.log(0, "ERR| query fetch fail [%s] [ret:%s]" % (sql, ret))
            return (0, '', '', '')

        sql = "UPDATE T_CMD_REQUEST SET state='INVOKE' WHERE id='%s'" % r_list[0]
        self.db.query(sql)
        self.lock.release()

        return (ret, r_list[0], r_list[1], r_list[2])

    def serve(self):
        global _SERVICE_STATE
        while _SERVICE_STATE:
            time.sleep(1)
            self.lock.acquire()
            self.proc.run(self.db, 'BCOMMANDER') 
            self.lock.release()    
            (ret, key, cmd_type, cmd) = self._get_command()

            if ret < 0:
                break

            if ret == 0:
                continue

            inst = ActionFactory.create_instance(cmd_type)
            inst.init(self.L, self.db, self.lock, key, cmd)
            inst.start()

    def clear(self, err_val):
        self.proc.down(self.db, 'BCOMMANDER')
        global _SERVICE_STATE
        _SERVICE_STATE = False
        self.db.close()

        if err_val:
            self.L.log(0, 'ERR| [%s]' % err_val)

        self.L.log(0, 'WRN| ============== clear end')

if __name__ == '__main__':

    bproc.ProcState.make_daemon()

    s = BCommander()

    s.init()
    try:
        s.serve()
        err_val = ''
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_val = traceback.format_exception(exc_type,
					     exc_value,
					     exc_traceback)
        print 'ERR| [%s]' % err_val
    finally:
        s.clear(err_val)

    sys.exit(0)

