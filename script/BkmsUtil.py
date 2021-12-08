# coding: utf-8

import sys
import os

from BASE import butil

class WorkItem(object):
    def __init__(self):
        self.item_id        = ''
        self.protocol       = ''
        self.method         = ''
        self.local_position = ''
        self.remote_position= ''

    def set(self, db, item_id, reg_date, L):
        sql = "SELECT item_id, work_protocol, work_method, "
        sql +=" local_position, remote_position FROM T_WORK_BOARD "
        sql +="WHERE item_id=%s AND reg_time='%s'" % (item_id, reg_date)

        ret = db.query(sql)

        if ret < 0:
            L.log(0, 'WRN| WorkItem:: query fail [%s]' % sql)
            self.unset()
            raise butil.ProxyDBErr('system select error')
   
        ret = db.fetch()

        if not len(ret):
            L.log(0, 'WRN| WorkItem:: not found system [%s]' % sql)
            self.unset()
            raise butil.ProxyDBErr('system not found')
 
        self.item_id            = ret[0]
        self.protocol           = ret[1]
        self.method             = ret[2]
        self.local_position     = ret[3]
        self.remote_position    = ret[4]


class System(object):
    def __init__(self):
        self.name       = ''
        self.ip         = ''
        self.port       = ''
        self.user       = ''
        self.passwd     = ''
        self.os_name    ='' 

    def unset(self):
        self.__init__()

    def set(self, db, item_id, L):
        sql = "SELECT s.name, s.ip, s.ftp_port, s.user, s.passwd, s.os_name "
        sql += " FROM T_SYSTEM AS s "
        sql += " WHERE s.name = (SELECT system_name FROM T_BK_ITEM AS i "
        sql += "  WHERE i.id= %s)" % item_id
        
        ret = db.query(sql)

        if ret < 0:
            L.log(0, 'WRN| System:: query fail [%s]' % sql)
            self.unset()
            raise butil.ProxyDBErr('system select error')
   
        ret = db.fetch()

        if not len(ret):
            L.log(0, 'WRN| System:: not found system [%s]' % sql)
            self.unset()
            raise butil.ProxyDBErr('system not found')
 
        self.name   = ret[0]
        self.ip     = ret[1]
        self.port   = ret[2]
        self.user   = ret[3]
        self.passwd = ret[4]
        self.os_name= ret[5]

class Progress(object):
    def __init__(self):
        self.state = 'ING'

        self.t_size = 0L 
        self.t_file_cnt = 0 
        self.c_size = 0L 
        self.c_file_cnt = 0 

        self.L     = None
        self.db    = None
        self.alarm = None

    def init(self, L, db, alarm):
        self.L     = L
        self.db    = db
        self.alarm = alarm

    def is_start(self):
        return self.c_size != 0

    def is_end(self):
        return self.alarm.was_occur() or self.t_size <= self.c_size


    def start(self, item_id, reg_date):
        sql = "SELECT try_cnt FROM T_WORK_BOARD "
        sql +=" WHERE item_id='%s' AND reg_time='%s'" % (item_id, reg_date)

        ret = self.db.query(sql)
        cnt = self.db.fetch()
        try_cnt = int(cnt[0]) + 1

        sql = "UPDATE T_WORK_BOARD SET state='ING', "
        sql += " result='',reason='', start_time=NOW(), try_cnt=%s" % (try_cnt)
        sql += " WHERE item_id=%s AND reg_time='%s'" % (item_id, reg_date)
        sql += " AND state='INVOKE'"

        ret = self.db.query(sql)
    
        if ret <= 0 :
            self.L.log(0, 'WRN| start query ret=%s sql=%s' % (ret, sql))
            raise butil.ProxyDBErr('progress start error')
        
        #self.L.log(1, 'INF| start query [%s]' % sql)

    def save_hist(self, item_id, reg_date):

        sql = "SELECT b.item_id, i.name, s.name, b.state, b.result, b.reason, "
        sql += "b.start_time, b.end_time, b.try_cnt, b.work_protocol, "
        sql += "b.work_method, b.local_position, b.remote_position, "
        sql += "b.t_size, b.t_file_cnt, b.c_size, b.c_file_cnt "
        sql +=" FROM T_WORK_BOARD AS b, T_BK_ITEM AS i, T_SYSTEM AS s "
        sql += " WHERE item_id=%s AND reg_time='%s' " % (item_id, reg_date) 
        sql += " AND b.item_id=i.id AND i.system_name=s.name"            

        ret = self.db.query(sql)

        if ret < 0:
            self.L.log(0, 'WRN| can not select term data [ret:%s]')

        n = self.db.fetch()

        if not len(n):
            L.log(0, 'WRN| save_hist:: not found WorkItem [%s]' % sql)
            self.unset()
            raise butil.ProxyDBErr('work item not found')

        item_id             = n[0]
        item_name           = n[1]
        system_name         = n[2]
        state               = n[3]
        result              = n[4]
        reason              = n[5]
        start_time          = n[6]
        end_time            = n[7]
        try_cnt             = n[8]
        work_protocol       = n[9]
        work_method         = n[10]
        local_position      = n[11]
        remote_position     = n[12]
        t_size              = n[13]
        t_file_cnt          = n[14]
        c_size              = n[15]
        c_file_cnt          = n[16]

        sql = "INSERT INTO T_WORK_HIST (item_id,item_name,system_name,"
        sql +=" state,result,reason,start_time,end_time,try_cnt,"
        sql +=" work_protocol,work_method,local_position,remote_position, "
        sql +=" t_size,t_file_cnt,c_size,c_file_cnt)"
        sql +=" VALUES (%s,'%s','%s','%s','%s','%s','%s','%s',%s,'%s'," % \
            (item_id, item_name, system_name, state, result, reason,
             start_time, end_time, try_cnt, work_protocol)
        sql +=" '%s','%s','%s',%s,%s,%s,%s)" % (work_method, 
                                           local_position,
                                           remote_position,
                                           t_size,t_file_cnt,c_size,c_file_cnt)

        ret = self.db.query(sql)

        self.L.log(3,
          'DEG| [apply_hist] insert [item_name:%s] [ret:%d]' % (item_name, ret))


    def term(self, item_id, reg_date, work, event):

        self.L.log(3, 'DEG| Progress::term ########### called start')

        sql="UPDATE T_WORK_BOARD SET state='TERM',reason='%s'," % \
            (event.err_reason)

        if event.is_success:
            sql += "result='SUCCESS',local_position='%s',remote_position='%s'," % \
                (work._work_real_local_dir, work._work_real_remote_dir)
        else:
            sql += "result='FAIL',"
   
        sql+=  "end_time=NOW(), t_size=%s, c_size=%s, " % \
                (work.total_size, work.work_size)
        sql += "target_cpu=%s,target_mem=%s,target_disk=%s,target_net=%s " % \
                (event.get_value('CPU'), 
                 event.get_value('MEM'),
                 event.get_value('DISK'),
                 event.get_value('NET'))
        sql+=" WHERE item_id=%s AND reg_time='%s'" % (item_id, reg_date)

        ret = self.db.query(sql)

        if ret <= 0 :
            self.L.log(0, 'WRN| term query ret=%s sql=%s' % (ret, sql))
            raise butil.ProxyDBErr('progress start error')

        # HISTORY 저장
        self.save_hist(item_id, reg_date)       
 
        self.L.log(1, 'INF| term query [%s]' % sql)

    def update(self, item_id, reg_date, work, event):

        sql = "UPDATE T_WORK_BOARD SET state='ING', "
        sql += "t_size=%s, c_size=%s, " % (work.total_size, work.work_size) 
        sql += "target_cpu=%s,target_mem=%s,target_disk=%s,target_net=%s " % \
                (event.get_value('CPU'), 
                 event.get_value('MEM'),
                 event.get_value('DISK'),
                 event.get_value('NET'))
        sql += " WHERE item_id=%s AND reg_time='%s'" % (item_id, reg_date)

        ret = self.db.query(sql)
   
        ## 여기서 수차례 변하지 않는지 여부를 확인할 수 있겠네요..횽횽
        if ret < 0 :
            self.L.log(0, 'WRN| update query ret=%s sql=%s' % (ret, sql))
            raise butil.ProxyDBErr('progress update error')
        
        self.L.log(1, 'INF| Progress::update [%d]' % ret)

    def is_force_term(self, item_id, reg_date):

        # select 하여, TERM 상태이면 종료한다.
        # 하던 작업 다 삭제하고 .. 뭐 그래야 겠죠..
        sql = "SELECT state FROM T_WORK_BOARD "
        sql +=" WHERE item_id=%s AND reg_time='%s' AND state='TERM'" % \
               (item_id, reg_date)

        ret = self.db.query(sql)

        if ret < 0:
            self.L.log(0, 
                'WRN| Progress:is_force_term select query ret=%s sql=%s' % \
                (ret, sql))
            raise butil.ProxyDBErr('progress select error')
        
        self.L.log(1, 'INF| Progress::is_force_term [%d]' % ret)
          
        if ret == 0:
            return False

        return True


class SystemEvent(object):
    def __init__(self):
        self.event_list = {}
        self.event_list['CPU'] = 0
        self.event_list['MEM'] = 0
        self.event_list['NET'] = 0        

        self.limit = {}
        self.limit['CPU'] = None 
        self.limit['MEM'] = None 
        self.limit['NET'] = None 

        self.value = {}
        self.value['CPU'] = 0
        self.value['MEM'] = 0
        self.value['NET'] = 0

        self.grade = 0

    def set_limit(self, tag, minor, major, critical):
        self.limit[tag] = [minor, major, critial]

    def sleep_time(self):
        return self.grade * 3

    def collect_cpu(self, val):
        self.__update_val__('CPU', val)

    def collect_mem(self, val):
        self.__update_val__('MEM', val)

    def collect_net(self, val):
        self.__update_val__('NET', val)

    def __update_val__(self, tag, val):
        self.event_list[tag] = 0

        # limit check
        for i in range(3): 
            if int(val) >= int(self.limit[tag][i]):
                self.event_list[tag] += 1

        max = 0
        for i in range(4):
            if self.event_list[i] > max:
                max = self.event_list[i]

        self.grade = max

        if int(val) > int('100'):
            val = '100'

        self.value[tag] = val

    def get_value(self, tag):
        ret = '0'
        try:
            ret = self.value[tag]
        except:
            pass

        return ret

