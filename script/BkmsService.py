# coding: utf-8

import sys
import time
import traceback
import ConfigParser

import BkmsUtil
from BASE import butil
from WORKER import BkmsWork
from WORKER import BkmsEvent
from WORKER import BkmsError

class BkmsService(object):
    def __init__(self, iLog):

        self.L      = iLog
        self.info   = {}

        self.work   = None
        self.event  = BkmsEvent.BkmsEvent(self.L) 

        self.progress=BkmsUtil.Progress()
        self.db     = butil.DbProxy()
        self.alarm  = butil.Alarm()

        self.item_id  = ''
        self.reg_time = ''

    def clear(self):
        try:
            self.work.clear()
            self.event.clear()
        except:
            pass

    def service(self):

        self.progress.init(self.L, self.db, self.alarm)


        try:
            self.progress.start(self.item_id, self.reg_time)
        except: 
            return

        self.work = BkmsWork.get_instance(self.L, self.info, self.event)
        self.work.create_telnet_thread(self.event)
     
        try:
            self.work.start()
        except BkmsError.Error, strerror:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| BkmsService::work start except : %s' \
                % repr(traceback.format_exception(exc_type, 
                                                  exc_value, 
                                                  exc_traceback)))
            self.event.set_terminate(False, strerror)

            try: 
                self.progress.term(self.item_id, self.reg_time,
                                     self.work, self.event)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsService::work start except : %s' \
                    % repr(traceback.format_exception(exc_type, 
                                                      exc_value, 
                                                      exc_traceback)))
            return

        while(self.event.is_service()):
            try:
                self.progress.update(self.item_id, self.reg_time, 
                                     self.work,    self.event)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsService::update : %s' \
                    % repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback))) 
                self.event.set_terminate(False, 'Failed to update the status')
                break

            if self.progress.is_force_term(self.item_id, self.reg_time):
                self.event.set_terminal(False, 'Forced by user')
                break

            self.work.speed_control()
            time.sleep(1)

        try:
            self.progress.term(self.item_id, self.reg_time, 
                               self.work,    self.event)
        except:
            pass

        self.work.arrange_result()

        return 


    def init(self, argv): 

        self.item_id = argv[1].strip()
        self.reg_time = argv[2].strip()
   
        try:
            config = ConfigParser.RawConfigParser()
            config.read(butil._MACRO['CONFIG_FILE'])
            proxy_ip = config.get('DB_PROXY', 'PROXY_IP')
            proxy_port = config.get('DB_PROXY', 'PROXY_PORT')
            config = None

            self.L.log(0, 'INF| proxy [ip:%s, port:%s]' % \
                (proxy_ip, proxy_port))

            self.db.set_log(self.L)
            self.db.open(proxy_ip, proxy_port)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_val = traceback.format_exception(exc_type,
                                                 exc_value,
                                                 exc_traceback)
            self.L.log(3, 'ERR| except %s' % err_val)
            self.L.log(0, 'ERR| except %s' % err_val.pop())

        sql = "SELECT s.name,w.work_protocol,w.work_method,w.local_position," 
        sql+= " w.remote_position, s.ip, s.telnet_port, s.user, s.passwd, "
        sql+= " s.ftp_port, s.tbd_port FROM T_WORK_BOARD AS w, T_SYSTEM AS s "
        sql+= " WHERE w.item_id=%s AND reg_time='%s' AND s.name = " % \
            (self.item_id, self.reg_time)
        sql+= " (SELECT system_name FROM T_BK_ITEM WHERE id=%s)" % self.item_id

        ret = self.db.query(sql)

        if ret <= 0:
            self.L.log(0, 'ERR| can not get data [ret:%s]')

            self.alarm.occur(butil._ALARM[401],'WORK INFO GET ERR [%s:%s]' % \
                    (self.item_id, self.reg_time))
            return

        r_list = self.db.fetch()
        if not r_list:
            self.L.log(0, 'ERR| can not get data [ret:%s]')

            self.alarm.occur(butil._ALARM[401],'WORK INFO NOT FOUND [%s:%s]' % \
                    (self.item_id, self.reg_time))
            return
            
        system_name   = r_list[0]
        work_protocol = r_list[1]
        work_method   = r_list[2]
        local_position= r_list[3]
        remote_position=r_list[4]
        ip            = r_list[5]
        t_port        = r_list[6]
        user          = r_list[7]
        passwd        = r_list[8]
        f_port        = r_list[9]
        tbd_port      = r_list[10]

        self.info['SYS_NAME'] = system_name
        self.info['HOW_WORK'] = work_protocol

        self.info['T_IP']     = ip
        self.info['T_PORT']   = t_port
        self.info['USER']     = user
        self.info['PASSWD']   = passwd

        self.info['WORK_TYPE']= work_method

        if work_method == 'BK':
            self.info['S_DIR']   = remote_position
            self.info['T_DIR']   = local_position
        else:
            self.info['S_DIR']   = local_position
            self.info['T_DIR']   = remote_position

        if work_protocol == 'FTP':
            self.info['W_PORT']  = f_port
        else:
            self.info['W_PORT']  = tbd_port
             
        self.info['HOST_NAME']   = 'BKMS'

        self.L.log(0, 'system name  : %s' % system_name) 
        self.L.log(0, 'work protocol: %s' % work_protocol) 
        self.L.log(0, 'work method  : %s' % work_method) 
        self.L.log(0, 'ip           : %s' % ip) 
        self.L.log(0, 'telnet port  : %s' % t_port) 
        self.L.log(0, 'work port    : %s' % self.info['W_PORT']) 

        self.L.log(0, 'user     : %s' % user) 
        self.L.log(0, 'source   : %s' % self.info['S_DIR']) 
        self.L.log(0, 'target   : %s' % self.info['T_DIR']) 

        return True

