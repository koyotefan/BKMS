# coding: utf-8

import threading
import select
import Queue
import time
import ConfigParser
import socket
import traceback
import sys
import datetime
import os
import subprocess

import KytLog
from BASE import butil
from BASE import bproc
import BkmsUtil

_SERVICE_STATE = True

class WorkQueue(object):
    def __init__(self):
        self.q = {}

    def add_type(self, key):
        self.q[key] = Queue.Queue()

    def get(self, key):
        return self.q[key].get()

    def put(self, key, value):
        self.q[key].put(value)


class Producer(threading.Thread):
    def __init__(self, work_queue, key):
        threading.Thread.__init__(self)

        self.q = work_queue.q[key]

        self.L = KytLog.Log(3)
        self.L.init(butil._MACRO['LOG_PATH'], 'producer.log') 

    def clear(self):
        global _SERVICE_STATE
        _SERVICE_STATE = False

    def run(self):

        self.L.log(0, '==================================================')
        self.L.log(0, 'INF| run start')

        global _SERVICE_STATE
        while _SERVICE_STATE:

            # self.L.log(0, 'INF| queue ready for reading')

            try:
                indent = self.q.get(True, 10)
            except Queue.Empty:
                continue

            self.L.log(0, 'INF| get indent %s' % indent)

            prog = os.path.join(os.getenv('HOME'),'app/script/worker.py')

            self.L.log(0, 'INF| exec file name : %s' % prog)

            child = subprocess.Popen(["python", 
                                      prog,
                                      indent.item_id,
                                      indent.reg_date,
                                      indent.system_name])
            self.L.log(0, 'INF| child run %s' % child.pid)

            while _SERVICE_STATE:
                try: time.sleep(1)
                except: break

                retcode = child.poll()

                self.L.log(0, 
                    'INF| child down [%s] ret[%s]' % (child.pid, retcode))
                break

        self.L.log(0, 'WRN| terminate')
        self.L.log(0, '')

    def serve(self):
        self.start()

class ScheduleDay(object):
    def __init__(self, diff):
        self.tday = datetime.date.today() + datetime.timedelta(diff)

    def querter(self):
        return (self.tday.month - 1) / 3

    def month(self):
        return self.tday.month

    def week(self):
        # mon = 0, tue = 1, ... sun = 6
        return self.tday.weekday()

    def day(self):
        return self.tday.day


class Scheduler(threading.Thread):
    def __init__(self, work_queue):
        threading.Thread.__init__(self)
        self.is_loaded = False  # 06:00 False, 18:00 , loading 후 True

        self.db= butil.DbProxy()
        self.q = work_queue 

        self.L = KytLog.Log(3)
        self.L.init(butil._MACRO['LOG_PATH'], 'scheduler.log') 

        # MACRO
        self.SCHD_TERM_TIME = butil._MACRO['TERM_TIME'] 
        self.SCHD_RUN_TIME  = butil._MACRO['RUN_TIME']

    def clear(self):
        global _SERVICE_STATE
        _SERVICE_STATE = False
        self.db.close()

    def _apply_hist(self):
        sql = "SELECT max(end_time) FROM T_WORK_HIST"

        ret = self.db.query(sql)

        if ret <= 0:
            self.L.log(0, 
                    'WRN| can not get max end_time from T_WORK_HIST [ret:%s]')
            return

        max_end_time = self.db.fetch()[0]
        self.L.log(3, 'DEG| get max end_time [%s]' % max_end_time)
        
        sql = "SELECT b.item_id, i.name, s.name, b.state, b.result, b.reason, "
        sql += "b.start_time, b.end_time, b.try_cnt, b.who, b.work_protocol, "
        sql += "b.work_method, b.local_position, b.remote_position, b.t_size, b.t_file_cnt, b.c_size, b.c_file_cnt "
        sql +="FROM T_WORK_BOARD AS b, T_BK_ITEM AS i, T_SYSTEM AS s "
        sql += " WHERE state = 'TERM' AND end_time > '%s' " % max_end_time
        sql += " AND b.item_id=i.id AND i.system_name=s.name" 

        ret = self.db.query(sql)

        if ret < 0:
            self.L.log(0, 'WRN| can not select term data [ret:%s]') 
            return

        work_term_list = self.db.fetch_all()

        for n in work_term_list:
            
            item_id             = n[0]
            item_name           = n[1]
            system_name         = n[2]
            state               = n[3]
            result              = n[4]
            reason              = n[5]
            start_time          = n[6]
            end_time            = n[7]
            try_cnt             = n[8]
            who                 = n[9]
            work_protocol       = n[10]
            work_method         = n[11]
            local_position      = n[12]
            remote_position     = n[13]
            t_size              = n[14]
            t_file_cnt          = n[15]
            c_size              = n[16]
            c_file_cnt          = n[17]

            sql = "INSERT INTO T_WORK_HIST (item_id,item_name,system_name,"
            sql +=" state,result,reason,start_time,end_time,try_cnt, who, "
            sql +=" work_protocol,work_method,local_position,remote_position,"
            sql +=" t_size,t_file_cnt,c_size,c_file_cnt)"
            sql +=" VALUES (%s,'%s','%s','%s','%s','%s','%s','%s',%s,'%s', " % \
                (item_id, item_name, system_name, state, result, reason,
                 start_time, end_time, try_cnt, who)
            sql +=" '%s','%s','%s',%s,%s,%s,%s)" % \
                   (work_protocol,work_method,local_position,remote_position, 
                    t_size, t_file_cnt, c_size, c_file_cnt)

            ret = self.db.query(sql)

            self.L.log(3, 
                'DEG| [apply_hist] insert [item_name:%s] [ret:%d]' % (item_name, ret))

    def _update_status_force_term(self):
        sql = "UPDATE T_WORK_BOARD SET state='TERM', result='FAIL', "
        sql += " reason='TIME EXPIRED', end_time=NOW() WHERE who='bkms' AND "
        sql += " (state='ING' OR state='INVOKE' or state='READY')"

        ret = self.db.query(sql)
        self.L.log(0, 'INF| update state force term [ret:%s]' % ret)

        if ret > 0:
            self._apply_hist()

    def _decide_loading_flag(self):
        sql = "SELECT * FROM T_WORK_BOARD WHERE "
        sql +=" who='bkms' AND reg_time > DATE_ADD(NOW(), INTERVAL -12 HOUR)"

        ret = self.db.query(sql)
        
        if ret > 0:
            self.is_loaded = True
            self.L.log(0, 'INF| have loaded backup item today [ret:%s]' % ret)
        else:
            self.is_loaded = False

    def _is_running_time(self):
        now_hour = time.strftime('%H', time.localtime())

        if now_hour >= self.SCHD_RUN_TIME or now_hour < self.SCHD_TERM_TIME:
            return True

        return False

    def _is_loading(self):

        if self._is_running_time():

            if not self.is_loaded:
                self.is_loaded = True

                self.L.log(0, 
                    'INF| it is loading time [loading time:%s]' % \
                     self.SCHD_RUN_TIME)
                return True

            if self.is_loaded:
                # Already Loaded
                return False

        if self.is_loaded:
            self.is_loaded = False
            self.L.log(2, 
                'INF| it is termination time [term time:%s]' % \
                self.SCHD_TERM_TIME)
            self._update_status_force_term()
            return False

        self.L.log(2, 'INF| is not running time [run time:%s-%s]' % \
                            (self.SCHD_RUN_TIME, self.SCHD_TERM_TIME))
        return False

    def _clear_before_loading(self):
        sql = "DELETE FROM T_WORK_BOARD "
        sql += " WHERE state='TERM' or state='IDLE'";

        ret = self.db.query(sql)

    def _loading_work_board(self, next_day):

        if next_day == 0:
            time_cond = "t.time_value >= '%s:00'" %  self.SCHD_RUN_TIME
        else:
            time_cond = "t.time_value < '%s:00'" % self.SCHD_TERM_TIME

        s_day = ScheduleDay(next_day)
        
        sql = "SELECT i.id, i.name, s.name, i.work_protocol, "
        sql +=" i.local_position, i.remote_position, t.time_value "
        sql +=" FROM T_BK_ITEM AS i, T_SYSTEM AS s, T_SCHD AS t WHERE "
        sql +=" (t.unit='DAY' OR "
        sql +=" (t.unit='WEEK' AND t.unit_value=%s) OR " % s_day.week() 
        sql +=" (t.unit='MONTH' AND t.day_value=%s) OR " % \
               (s_day.day()) 
        sql +=" (t.unit='QUARTER' AND t.unit_value=%s AND t.day_value=%s)) " % \
               (s_day.querter(), s_day.day()) 
        sql +=" AND i.id=t.item_id AND i.system_name=s.name AND %s" % time_cond

        ret = self.db.query(sql)

        if ret < 0:
            self.L.log(0, 'WRN| can not select work item')
            return

        work_list = self.db.fetch_all()
    
        for n in work_list:

            item_id             = n[0]
            item_name           = n[1]
            system_name         = n[2]
            work_protocol       = n[3]
            local_position      = n[4]
            remote_position     = n[5]
            schd_time           = n[6]

            (hour, minute, sec) = schd_time.split(':')
            if self.SCHD_RUN_TIME <= hour:
                str_schd_time = time.strftime('%Y%m%d') + hour + minute
            else:
                day = datetime.date.today() + datetime.timedelta(1)
                str_schd_time = day.strftime('%Y%m%d') + hour + minute

            sql = "INSERT INTO T_WORK_BOARD "
            sql +=" (item_id, reg_time, schd_time, who, "
            sql +="  work_protocol,work_method,local_position,remote_position) " 
            sql +=" VALUES ('%s',NOW(),STR_TO_DATE('%s', " % (item_id, str_schd_time)
            sql +=" '%Y%m%d%H%i'), " 
            sql +=" 'bkms', '%s', 'BK', '%s', '%s')" % \
                   (work_protocol, 
                    os.path.join(local_position, system_name, item_name), 
                    remote_position) 

            ret = self.db.query(sql)

    def _make_local_position(self, system_name, item_name):
        return os.path.join(butil._MACRO['DATA_PATH'],system_name,item_name) 

    def find_item(self): 

        sql = "SELECT w.item_id, w.reg_time, i.system_name FROM "
        sql +=" T_WORK_BOARD AS w, T_BK_ITEM AS i " 
        sql +=" WHERE schd_time <= NOW() AND (state='IDLE' OR state='RETRY') "
        sql +=" AND i.id=w.item_id"

        ret = self.db.query(sql)

        if ret <= 0:
            return []

        item_list = []
        while True:
            r_list = self.db.fetch()
            if not r_list:
                break

            self.L.log(0, 'INF| find_item list:%s' % r_list)
            item_list.append(r_list)

        return item_list

    def _update_state_invoke(self, item_id, reg_time):
        sql = "UPDATE T_WORK_BOARD SET state='INVOKE' "
        sql += " WHERE item_id=%s AND reg_time='%s'" % (item_id, reg_time)

        ret = self.db.query(sql)

        self.L.log(0, 'INF| update state INVOKE [ret:%s item:%s]' % \
                (ret, item_id))


    def _update_state_retry(self):
        sql = "UPDATE T_WORK_BOARD SET state='RETRY', "
        sql += " schd_time=DATE_ADD(NOW(), INTERVAL 1 HOUR) "
        sql += " WHERE state='TERM' AND result != 'SUCCESS' AND who='bkms' AND "
        sql += " reg_time > DATE_ADD(NOW(), INTERVAL -12 HOUR) AND "
        sql += " reason <> 'TIME EXPIRED'" 

        ret = self.db.query(sql)
        self.L.log(0, 'INF| update state RETRY [ret:%s]' % ret)

    def run(self): 

        global _SERVICE_STATE

        try:
            self.L.log(0, '==================================================')
            self.L.log(0, 'INF| run start')
            config = ConfigParser.RawConfigParser()
            config.read(butil._MACRO['CONFIG_FILE'])
            proxy_ip = config.get('DB_PROXY', 'PROXY_IP')
            proxy_port = config.get('DB_PROXY', 'PROXY_PORT')
            config = None

            self.L.log(0, 'INF| proxy [ip:%s, port:%s]' % \
                (proxy_ip, proxy_port))

            self.db.set_log(self.L)
            self.db.open(proxy_ip, proxy_port)
            indent = WorkIndent()
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            self.L.log(3, 'ERR| except %s' % err_val)
            self.L.log(0, 'ERR| except %s' % err_val.pop())
            _SERVICE_STATE = False

        self.L.log(0, 'INF| RUN TIME [%s] TERM TIME [%s]' % \
                (self.SCHD_RUN_TIME, self.SCHD_TERM_TIME))
        self._decide_loading_flag()

        while _SERVICE_STATE:
            try:
                self.L.log(2, 'INF| working')

                if self._is_loading():
                    self._clear_before_loading()
                    self._loading_work_board(0)  # today work item
                    self._loading_work_board(1)  # next day work item

                if self._is_running_time():
                    self._update_state_retry()

                item_list = self.find_item()

                for item in item_list:
                    indent.make(item, self.db, self.L)

                    if indent.validate():
                        self._update_state_invoke(indent.item_id, 
                                                  indent.reg_date)
                        indent.send(self.q)
                        self.L.log(3, 'DEG| indet %s' % self.q)

                    else:
                        self.L.log(0, 'WRN| invalid indent %s' % indent)

                select.select([],[],[],10)

            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_val = traceback.format_exception(exc_type,
                                                     exc_value,
                                                     exc_traceback)
                self.L.log(3, 'ERR| serivce except %s' % err_val)
                self.L.log(0, 'ERR| serivce except %s' % err_val.pop())
                _SERVICE_STATE = False

        self.db.close() 
        self.L.log(0, 'WRN| terminate')
        self.L.log(0, '')

    def serve(self):
        self.start()

class WorkIndent(object):
    def __init__(self):
        self.item_id         = ''
        self.reg_date        = ''
        self.system_name     = ''

        # self.system = None

    def __str__(self):
        return 'item[%s] reg_date [%s] system_name [%s]' % (self.item_id, 
                                                            self.reg_date,
                                                            self.system_name)

    def make(self, item_list, db, L):
        # (item_id, reg_date)

        self.item_id        = item_list[0]
        self.reg_date       = item_list[1]
        self.system_name    = item_list[2]

        self.system = BkmsUtil.System() 
        self.system.set(db, self.item_id, L)

    def validate(self):
        if not self.item_id or \
           not self.reg_date :
            return False

        return True

    def send(self, work_queue):
        work_queue.put(self.system.ip.split('.')[0], self) # ip Class

    def recv(self, queue_dic, network_a_class):
        q = queue_dic[network_a_class] # ip A Class
        indent = q.get()

        self.item_id         = indent.item_id 
        self.reg_date        = indent.reg_date
        self.system_name     = indent.system_name


class BService(object):
    def __init__(self):
        self.queue = WorkQueue()
        self.queue.add_type('192')
        self.queue.add_type('150')
        self.queue.add_type('60')
        self.queue.add_type('10')

        self.L = KytLog.Log(3)
        self.L.init(butil._MACRO['LOG_PATH'], 'service.log')

        self.proc = None
        self.db = None

    def init(self):
        self.L.log(0, 'INF| init')
        self.pd1 = Producer(self.queue, '192')
        self.pd2 = Producer(self.queue, '150')
        self.pd3 = Producer(self.queue, '60')
        self.pd4 = Producer(self.queue, '10')

        self.s = Scheduler(self.queue)
        self.L.log(0, 'INF| init end')

    def serve(self):

        self.pd1.serve()
        self.L.log(0, 'INF| invoke producer1 serve ')
        self.pd2.serve()
        self.L.log(0, 'INF| invoke producer2 serve ')
        self.pd3.serve()
        self.L.log(0, 'INF| invoke producer3 serve ')
        self.pd4.serve()
        self.L.log(0, 'INF| invoke producer4 serve ')

        self.s.serve()
        self.L.log(0, 'INF| invoke scheduler serve ')

        self.db = butil.DbProxy()
        config = ConfigParser.RawConfigParser()
        config.read(butil._MACRO['CONFIG_FILE'])

        proxy_ip   = config.get('DB_PROXY', 'PROXY_IP')
        proxy_port = config.get('DB_PROXY', 'PROXY_PORT') 

        self.db.set_log(self.L)
        self.db.open(proxy_ip, proxy_port)

        self.proc = bproc.ProcState()

        global _SERVICE_STATE
        # Commander role
        while _SERVICE_STATE:
            try:
                time.sleep(5)
                self.L.log(0, 'INF| temp print')
                self.proc.run(self.db, 'BSERVICE')
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_val = traceback.format_exception(exc_type,
                                                     exc_value,
                                                     exc_traceback)
                self.L.log(3, 'ERR| except %s' % err_val)
                self.L.log(0, 'ERR| except %s' % err_val.pop())
                _SERVICE_STATE = False

        self.L.log(0, 'WRN| terminate')
        self.L.log(0, '')

    def clear(self, err_val):
        try:
            self.proc.down(self.db, 'BSERVICE')
            self.db.close()
        except:
            pass

        self.L.log(0, 'WRN| clear')
        self.s.clear()

        self.pd1.clear()
        self.pd2.clear()
        self.pd3.clear()
        self.pd4.clear()

        if err_val:
            self.L.log(0, 'ERR| [%s]' % err_val)

        self.L.log(0, 'WRN| ============== clear end')

if __name__ == '__main__':

    bproc.ProcState.make_daemon()

    s = BService()

    s.init()
    try:
        s.serve()
        err_val = ''
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_val = traceback.format_exception(exc_type,
                                             exc_value,
                                             exc_traceback)
    finally:
        s.clear(err_val)

    sys.exit(0)

