
from socket import *
import time

import BkmsError
import BkmsMessage
import sys
import traceback

class BkmsClient(object):

    def __init__(self, iLog, port):
        self.host = 'localhost'
        self.port = port
        self.L    = iLog

        self.conn = None

        self.msg  = BkmsMessage.BkmsMessage()

    def connect(self):
        if self.conn:
            try: self.clear()
            except: pass

        try:
            self.conn = socket(AF_INET, SOCK_STREAM)
            self.conn.connect((self.host, self.port))
        except:
            raise BkmsError.err_bkms_client('CMD_CONN_FAIL')

    def clear(self):
        try:
            self.conn.close()
        except:
            pass 
        finally:
            self.conn = None

    def allow(self, work, event):
        
        self.connect()

        cmd = work.get_allow_cmd()

        self.msg.enc_header(cmd, 
                            '0000', 
                            '3000', 
                            BkmsMessage._MSG_LEN['ALLOW'], 
                            '0000')

        self.msg.enc_allow_body(work.schd_code, 
                                work.schd_code, 
                                time.strftime('%Y%m%d'), 
                                work.remote_dir,
                                work.remote_dir,
                                '')

        self.send(self.msg.get_message())

        data = self.recv(BkmsMessage._MSG_LEN['HEADER'])
        self.msg.dec_header(data) 
        
        if self.msg.get_header('CMD') != cmd:
            raise BkmsError.err_not_allow
        
        data = self.recv(self.msg.get_header('LEN'))

        self.msg.dec_allow_body(data)

        work.work_date = self.msg.get_body('WORK_DATE')
        work.work_time = self.msg.get_body('WORK_TIME') 

        event.set_cpu_limit(self.msg.get_body('L_CPU'),
                           self.msg.get_body('M_CPU'),
                           self.msg.get_body('H_CPU'))
        event.set_mem_limit(self.msg.get_body('L_MEM'),
                           self.msg.get_body('M_MEM'),
                           self.msg.get_body('H_MEM'))
        event.set_disk_limit(self.msg.get_body('L_DISK'),
                           self.msg.get_body('M_DISK'),
                           self.msg.get_body('H_DISK'))
        event.set_net_limit(self.msg.get_body('L_NET'),
                           self.msg.get_body('M_NET'),
                           self.msg.get_body('H_NET'))


    def send_report(self, work, event):

        cmd_code = work.get_cmd()

        (in_service, is_success, err_reason) = event.get_terminate()

        if not in_service:
            end_flag = 'Y'

            if not is_success:
                try:
                    cmd_code = BkmsMessage._MSG_CMD[str(err_reason)]
                except:
                    cmd_code = BkmsMessage._MSG_CMD['CMD_FTP_SYS_FAIL']
        else:
            end_flag = 'N'

        work_size = str(work.work_size)
        total_size = str(work.total_size)

        self.L.log(0, 
            'INF| BkmsClient::send_report code:%s msg len:%s' % \
            (cmd_code, BkmsMessage._MSG_LEN['BODY']))
        self.L.log(0, 
            'INF| BkmsClient::send_report sys:%s target:%s size:%s tot:%s'\
            % (work.system, work.target, work_size, total_size))
        self.L.log(0, 
            'INF| BkmsClient::send_report cpu:%s mem:%s net:%s disk:%s'\
            % (event.get_value('CPU'), event.get_value('MEM'),
                event.get_value('NET'), event.get_value('DISK')))


        self.msg.enc_header(cmd_code, 
                            '', 
                            '', 
                            BkmsMessage._MSG_LEN['BODY'], 
                            '')
        self.msg.enc_body(work.system,
                            'TBD/dbsu_bdutil',
                            '',
                            work.target,
                            '',
                            '644',
                            '',
                            '',
                            '',
                            work_size,
                            total_size,
                            end_flag,
                            str(event.get_value('CPU')),
                            str(event.get_value('MEM')),
                            str(event.get_value('NET')),
                            str(event.get_value('DISK')),
                            '0')

        self.send(self.msg.get_message())

    def recv_report(self, work, event):
        data = self.recv(BkmsMessage._MSG_LEN['HEADER'])
       
        self.msg.dec_header(data)

        if self.msg.get_header('CMD') != BkmsMessage._MSG_CMD['CMD_BDM_BKUP'] and \
        self.msg.get_header('CMD') != BkmsMessage._MSG_CMD['CMD_BDM_DUMP']: 
            self.L.log(0, 
                'INF| BkmsClient::recv_report unknown cmd recv : %s' % \
                self.msg.get_header('CMD'))

            raise BkmsError.err_service_term(self.msg.get_header('CMD'))
       
    def send(self, data):
        if not self.conn:
            self.connect()

        self.L.log(2, 'INF| send [%s]' % data)
        try:
            self.conn.send(data)                                             
        except:                                                              
            raise BkmsError.err_send

    def recv(self, size):
        if not self.conn:
            self.connect()

        data = ''

        try:
            data = self.conn.recv(size)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| BkmsClient::recv error :%s" \
                 % repr(traceback.format_exception( exc_value, exc_value,
                                                    exc_traceback)))
            raise BkmsError.err_recv

        self.L.log(2, 'INF| recv [%s]' % data)

        if len(data) != size:
            self.L.log(0, 
              'ERR| BkmsClient::recv length fail read size:%d, req size:%d' % \
                (len(data), size))
            raise BkmsError.err_recv

        return data

        
