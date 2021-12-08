
#coding: utf-8

import re
import time
import ConfigParser
import traceback
import sys
import os
import subprocess

import KytLog
from BASE import butil
from BASE import bproc

_SERVICE_STATE = True

class ResourceHW(object):
    def __init__(self, iLog):
        self.L = iLog
        
        self.total_mem_size = 1
        self.disk_usage_list = []
        self.mem_usage = ''
        self.cpu_usage = ''

    def init(self):
        # prfconf | grep Memory
        try:
            cmd = 'prtconf | grep Memory'
            p = subprocess.Popen(cmd,
                                 shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            line_list = []
            for line in p.stdout.readlines():
                line_list.append(line)

            retval = p.wait()

            ret = line_list.pop()
            self.total_mem_size = re.search('[0-9]+', ret).group()

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| run except %s' % err_val)
            self.L.log(0, 'ERR| run except %s' % last_err_val) 
            return False

        self.L.log(0, 'INF| total mem size %s' % self.total_mem_size)
        return True

    # sar 1 3
    def get_cpu(self):
        try:
            cmd = 'sar 1 3'
            p = subprocess.Popen(cmd,
                                 shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            line_list = []
            for line in p.stdout.readlines():
                line_list.append(line)

            retval = p.wait()

            ret = line_list.pop()
            
            self.cpu_usage = ret.split()[-1:][0] 

            self.cpu_usage = '%s' % (100 - int(self.cpu_usage)) 

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| run except %s' % err_val)
            self.L.log(0, 'ERR| run except %s' % last_err_val) 
            return False

        return True
 
    # vmstat 1 3
    def get_mem(self):
        try:
            cmd = 'vmstat 1 3'
            p = subprocess.Popen(cmd,
                                 shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            line_list = []
            for line in p.stdout.readlines():
                line_list.append(line)

            retval = p.wait()

            ret = line_list.pop()

            val = 1 - 1.0 * \
                int(ret.split()[4])/(long(self.total_mem_size) * 1024)            
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| run except %s' % err_val)
            self.L.log(0, 'ERR| run except %s' % last_err_val) 
            return False

        if int(val * 100) >= 0 and int(val * 100) <= 100:
            self.mem_usage = str(int(val * 100))

        return True
 
    def get_disk(self):
        try:
            cmd = 'df -k'
            p = subprocess.Popen(cmd,
                                 shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            self.disk_usage_list = []
            for line in p.stdout.readlines():
                if not '%' in line:
                    continue
                
                self.disk_usage_list.append((line.split()[-1:][0], line.split()[-2:-1][0]))

            retval = p.wait()

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            last_err_val = err_val.pop()
            self.L.log(3, 'ERR| run except %s' % err_val)
            self.L.log(0, 'ERR| run except %s' % last_err_val) 
            return False

        return True
 
    def save(self, db):

        sql = "DELETE FROM T_RESOURCE_BOARD" 
        db.query(sql)

        sql = "INSERT INTO T_RESOURCE_BOARD (update_time, `type`, `name`, `value`) VALUES "

        sql_cpu = sql + " (NOW(), 'CPU', 'CPU', '%s')" % self.cpu_usage
        db.query(sql_cpu)

        sql_mem = sql + " (NOW(), 'MEM', 'MEM', '%s')" % self.mem_usage
        db.query(sql_mem)

        for v in self.disk_usage_list: 
            sql_disk= sql + " (NOW(), 'DISK', '%s', '%s')" % (v[0], v[1])
            db.query(sql_disk)


class BSupervisor(object):
    def __init__(self):
        self.L = KytLog.Log(3)
        self.L.init(butil._MACRO['LOG_PATH'], 'supervisor.log')

        self.proxy_ip = ''
        self.proxy_port = ''
        self.proc = None

        self.db = None

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

        self.proc = None

    def serve(self):
        self.proc = bproc.ProcState()

        # Resource Check
        resourceHW = ResourceHW(self.L)
        resourceHW.init()
       
        global _SERVICE_STATE

        while _SERVICE_STATE:
            resourceHW.get_cpu()
            resourceHW.get_mem()
            resourceHW.get_disk()

            resourceHW.save(self.db)

            try:
                self.proc.run(self.db, 'BSUPERVISOR')
                time.sleep(5)
                
            except:
                return
 
        # Proc Check

        # HW Check

        # Delete Log / Data

        # move Data

    def clear(self, err_val):
        self.proc.down(self.db, 'BSUPERVISOR')
        global _SERVICE_STATE
        _SERVICE_STATE = False
        self.db.close()

        if err_val:
            self.L.log(0, 'ERR| [%s]' % err_val)

        self.L.log(0, 'WRN| ============== clear end')

if __name__ == '__main__':

    bproc.ProcState.make_daemon()
    
    s = BSupervisor()

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

