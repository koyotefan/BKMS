
import time
import sys
import traceback
import threading

import BkmsError
import BkmsFtp
import BkmsRemoteResource
import BkmsTelnet
import BkmsDir

def get_instance(iLog, info, event):

    if info['HOW_WORK'] == 'FTP' and info['WORK_TYPE'] == 'BK':
        return FTPDownWork(iLog, info, event)
    elif info['HOW_WORK'] == 'FTP' and info['WORK_TYPE'] == 'RS':
        return FTPUpWork(iLog, info, event)
    elif info['HOW_WORK'] == 'TBD' and info['WORK_TYPE'] == 'BK':
        return TBDDownWork(iLog, info, event)
    elif info['HOW_WORK'] == 'TBD' and info['WORK_TYPE'] == 'RS':
        return TBDUpWork(iLog, info, event)
    else:
        return None

class BkmsWork(object):

    def __init__(self, info):
        self.system     = info['SYS_NAME']
        self.ip         = info['T_IP']
        self.t_port     = info['T_PORT']
        self.tbd_port   = info['W_PORT']
        self.user       = info['USER']
        self.pwd        = info['PASSWD']
        self.source     = info['S_DIR']
        self.target     = info['T_DIR']

        self.work_date  =  time.strftime('%Y%m%d')
        self.work_time  =  time.strftime('%H%M%S') 

        self._work_real_local_dir = ''
        self._work_real_remote_dir= ''

        self.total_size = 0L
        self.work_size  = 0L

            
    def create_telnet_thread(self, event):
        try:        

            remote_resource = \
                BkmsRemoteResource.BkmsRemoteResource(self.L, event)

            remote_resource.init(self.ip, self.t_port, self.user, self.pwd, None)

            thr_telnet_cpu = \
                threading.Thread(target=remote_resource.cpu, args=(),)
            thr_telnet_cpu.setDaemon(True)
            thr_telnet_cpu.start()

            thr_telnet_mem = \
                threading.Thread(target=remote_resource.memory, args=(),)
            thr_telnet_mem.setDaemon(True)
            thr_telnet_mem.start()

            thr_telnet_disk = \
                threading.Thread(target=remote_resource.disk, args=(),)
            thr_telnet_disk.setDaemon(True)
            thr_telnet_disk.start()

            thr_telnet_net = \
                threading.Thread(target=remote_resource.network, args=(),)
            thr_telnet_net.setDaemon(True)
            thr_telnet_net.start()
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| Work::create_telnet_thread except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 

    def arrange_result(self):

        (in_service, is_success, err_reason) = self.event.get_terminate()

        if in_service == True:
            pass

        try:
            itelnet = BkmsTelnet.BkmsTelnet(self.L, self.ip, int(self.t_port))
            itelnet.login(self.user, self.pwd)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| Work::arrange_result except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 
            return

        iDir = BkmsDir.BkmsDir(self.L)

        # chmod
        if is_success == True:

            iDir.change_dir(itelnet, self.target_directory)

            for f in self.source_file_list:
                file_name = f[:-3]
                mode      = f[-3:]
                
                iDir.remote_chmod(itelnet, file_name, mode)

        else:
            iDir.remove_dir(itelnet, self.target_directory)

        itelnet.clear()


class FTPUpWork(BkmsWork):
    def __init__(self, iLog, info, event):
        super(FTPUpWork, self).__init__(info)

        self.L = iLog
        self.event = event

        self.local_dir  = self.source 
        self.remote_dir = self.target 

        self.itelnet = None

        self.target_directory = ''

        self.source_base_position  = ''
        self.source_directory_list = []
        self.source_file_list      = []

        self.f_port = info['W_PORT']

    def __ready__(self):
        try:
            self.itelnet = BkmsTelnet.BkmsTelnet(self.L, 
                                self.ip, int(self.t_port))
            self.itelnet.login(self.user, self.pwd)
        except:
            raise BkmsError.err_bkms_work('CMD_LOGIN_FAIL')

        iDir = BkmsDir.BkmsDir(self.L)

        self.target = iDir.abs_dir_for_tilde(self.itelnet, self.target)
        self.target = iDir.change_dir_for_env_variable(self.itelnet, self.target)
        # dont use up 
        #self.target = iDir.change_dir_for_link(self.itelnet, self.target)
        self.remote_dir = self.target
       
        (self.source_base_position, 
         self.source_directory_list, 
         self.source_file_list,
         self.total_size) = iDir.find_local_files(self.local_dir) 

        self._work_real_local_dir = self.source_base_position

        self.L.log(0, 
            'INF| FTPUpWork::ready source_base_position:%s'% \
            self.source_base_position)
        self.L.log(0,
            'INF| FTPUpWork::ready source_directory_list:%s' % \
            self.source_directory_list)
        self.L.log(0,
            'INF| FTPUpWork::ready source file cnt :%s' % \
            len(self.source_file_list))
        self.L.log(0,
            'INF| FTPUpWork::ready source total size :%s' % \
            self.total_size) 

        if len(self.source_file_list) == 0:
            raise BkmsError.err_bkms_work('CMD_DUMP_S_NOFILE')

        self.target_directory = \
         iDir.find_remote_target_position(self.itelnet, self.remote_dir)
      
        self._work_real_remote_dir = self.target_directory

        self.L.log(0,
            'INF| FTPUpWork::ready target directory :%s' % \
            self.target_directory) 

    def clear(self):
        try: self.itelnet.clear()
        except: pass

    def start(self):

        self.__ready__()
        ftp = BkmsFtp.BkmsFtp(self) 

        thr_ftp = threading.Thread(target=ftp.upFtp, args=(),)
        thr_ftp.setDaemon(True)
        thr_ftp.start()


    def create_telnet_thread(self, service):
        pass

    def speed_control(self):
        pass

class FTPDownWork(BkmsWork):
    def __init__(self, iLog, info, event):
        super(FTPDownWork, self).__init__(info)

        self.L = iLog
        self.event = event

        self.local_dir      = self.target 
        self.remote_dir     = self.source

        self.itelnet = None

        self.target_directory = ''

        self.source_base_position  = ''
        self.source_directory_list = []
        self.source_file_list      = []

        self.f_port  = info['W_PORT']

    def __ready__(self):
        try:
            self.itelnet = BkmsTelnet.BkmsTelnet(self.L, 
                                self.ip, int(self.t_port))
            self.itelnet.login(self.user, self.pwd)
        except:
            raise BkmsError.err_bkms_work('CMD_LOGIN_FAIL')

        '''
        self.itelnet = BkmsTelnet.BkmsTelnet(self.L, 
                                self.ip, int(self.t_port))
        self.itelnet.login(self.user, self.pwd)
        '''

        iDir = BkmsDir.BkmsDir(self.L)

        self.L.log(2, 'INF| BkmsWork::source : %s' % self.source)
        self.source = iDir.abs_dir_for_tilde(self.itelnet, self.source)
        self.L.log(2, 'INF| BkmsWork::abs : %s' % self.source)
        self.source = iDir.change_dir_for_env_variable(self.itelnet, self.source)
        self.L.log(2, 'INF| BkmsWork:: chg env var : %s' % self.source)
        self.source = iDir.change_dir_for_link(self.itelnet, self.source)
        self.L.log(2, 'INF| BkmsWork:: chg link : %s' % self.source)
        self.remote_dir = self.source
        # self.target used only send_report 
        self.target = self.source
        
        (self.source_base_position, 
         self.source_directory_list, 
         self.source_file_list,
         self.total_size) = iDir.find_remote_files(self.itelnet, 
                                                   self.user, 
                                                   self.remote_dir)


        self.L.log(0, 
            'INF| FTPDownWork::ready source_base_position:%s'% \
            self.source_base_position)
        self.L.log(0,
            'INF| FTPDownWork::ready source_directory_list:%s' % \
            self.source_directory_list)
        self.L.log(0,
            'INF| FTPDownWork::ready source file cnt :%s' % \
            len(self.source_file_list))
        self.L.log(0,
            'INF| FTPDownWork::ready source total size :%s' % \
            self.total_size) 

        self._work_real_remote_dir = self.source_base_position

        if len(self.source_file_list) == 0:
            raise BkmsError.err_bkms_work('CMD_BKUP_S_NOFILE')

        self.target_directory =  \
            iDir.find_local_target_position(self)

        self._work_real_local_dir = self.target_directory

        self.L.log(0,
            'INF| FTPDownWork::ready target directory :%s' % \
            self.target_directory) 

    def clear(self):
        try: self.itelnet.clear()
        except: pass

    def start(self):
        self.__ready__()
        ftp = BkmsFtp.BkmsFtp(self) 

        thr_ftp = threading.Thread(target=ftp.downFtp, args=(),)
        thr_ftp.setDaemon(True)
        thr_ftp.start()


    def create_telnet_thread(self, service):
        pass

    def speed_control(self):
        pass

    def arrange_result(self):
        pass

class TBDUpWork(BkmsWork):
    def __init__(self, iLog, info, event):
        super(TBDUpWork, self).__init__(info)

        self.L = iLog
        self.event = event
        self.ftp   = None

        self.local_dir  = self.source 
        self.remote_dir = self.target

        self.itelnet = None

        self.target_directory = ''

        self.source_base_position  = ''
        self.source_directory_list = []
        self.source_file_list      = []

        self.f_port   = self.tbd_port 

    def __ready__(self):
        try:
            self.itelnet = BkmsTelnet.BkmsTelnet(self.L, 
                                self.ip, int(self.t_port))
            self.itelnet.login(self.user, self.pwd)
        except:
            raise BkmsError.err_bkms_work('CMD_LOGIN_FAIL')

        iDir = BkmsDir.BkmsDir(self.L)

        self.target = iDir.abs_dir_for_tilde(self.itelnet, self.target)
        self.target = iDir.change_dir_for_link(self.itelnet, self.target)
        self.remote_dir = self.target

        (self.source_base_position, 
         self.source_directory_list, 
         self.source_file_list,
         self.total_size) = iDir.find_local_files(self.local_dir) 

        self.L.log(0, 
            'INF| TBDUpWork::ready source_base_position:%s'% \
            self.source_base_position)
        self.L.log(0,
            'INF| TBDUpWork::ready source_directory_list:%s' % \
            self.source_directory_list)
        self.L.log(0,
            'INF| TBDUpWork::ready source file cnt :%s' % \
            len(self.source_file_list))
        self.L.log(0,
            'INF| TBDUpWork::ready source total size :%s' % \
            self.total_size) 

        self._work_real_local_dir = self.source_base_position

        if len(self.source_file_list) == 0:
            raise BkmsError.err_bkms_work('CMD_DUMP_S_NOFILE')

        self.target_directory = \
            iDir.find_remote_target_position(self.itelnet, self.remote_dir)

        self._work_real_remote_dir = self.target_directory

        self.L.log(0,
            'INF| TBDUpWork::ready target directory :%s' % \
            self.target_directory) 

        self.itelnet.run_agent(self.ip, 
                               self.f_port,
                               self.user,
                               self.pwd)

    def clear(self):
        try: self.itelnet.clear()
        except: pass

    def start(self):

        self.__ready__()
        self.ftp = BkmsFtp.BkmsFtp(self) 

        thr_ftp = threading.Thread(target=self.ftp.upFtp, args=(),)
        thr_ftp.setDaemon(True)
        thr_ftp.start()

    def speed_control(self):
        pass

class TBDDownWork(BkmsWork):
    def __init__(self, iLog, info, event):
        super(TBDDownWork, self).__init__(info)
        self.L = iLog
        self.event = event
        self.ftp   = None

        self.local_dir  = self.target
        self.remote_dir = self.source

        self.itelnet = None

        self.target_directory = ''

        self.source_base_position  = ''
        self.source_directory_list = []
        self.source_file_list      = []

        self.f_port = self.tbd_port

    def __ready__(self):
        try:
            self.itelnet = BkmsTelnet.BkmsTelnet(self.L, 
                                self.ip, int(self.t_port))
            self.itelnet.login(self.user, self.pwd)
        except:
            raise BkmsError.err_bkms_work('CMD_LOGIN_FAIL')

        iDir = BkmsDir.BkmsDir(self.L)

        self.source = iDir.abs_dir_for_tilde(self.itelnet, self.source)
        self.source = iDir.change_dir_for_link(self.itelnet, self.source)
        self.remote_dir = self.source
        # self.target used only send_report 
        self.target = self.source
        
        (self.source_base_position, 
         self.source_directory_list, 
         self.source_file_list,
         self.total_size) = iDir.find_remote_files(self.itelnet,
                                                   self.user,
                                                   self.remote_dir) 
        self.L.log(0, 
            'INF| TBDDownWork::ready source_base_position:%s'% \
            self.source_base_position)
        self.L.log(0,
            'INF| TBDDownWork::ready source_directory_list:%s' % \
            self.source_directory_list)
        self.L.log(0,
            'INF| TBDDownWork::ready source file cnt :%s' % \
            len(self.source_file_list))
        self.L.log(0,
            'INF| TBDDownWork::ready source total size :%s' % \
            self.total_size) 

        self._work_real_remote_dir = self.source_base_position

        if len(self.source_file_list) == 0:
            raise BkmsError.err_bkms_work('CMD_BKUP_S_NOFILE')

        self.target_directory = \
            iDir.find_local_target_position(self)

        self._work_real_local_dir = self.target_directory

        self.L.log(0,
            'INF| TBDDownWork::ready target directory :%s' % \
            self.target_directory) 

        self.itelnet.run_agent(self.ip, 
                               self.f_port,
                               self.user,
                               self.pwd)

    def clear(self):
        try: self.itelnet.clear()
        except: pass

    def start(self):
        self.__ready__()
        self.ftp = BkmsFtp.BkmsFtp(self) 

        thr_ftp = threading.Thread(target=self.ftp.downFtp, args=(),)
        thr_ftp.setDaemon(True)
        thr_ftp.start()

    def speed_control(self):
        if self.event.is_event():
            self.L.log(0, 
               'TBDDownWork::speed control : %s' % self.event.sleep_time()) 
            self.ftp.tbd_speed_control(self.event.sleep_time())
        else:
            self.L.log(0, 'TBDDownWork::speed control : 0')  
            self.ftp.tbd_speed_control(0)

    def arrange_result(self):
        pass

