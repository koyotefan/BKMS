
import os
import time
import sys
import traceback
import ftplib

import BkmsError


class BkmsFtp(object):
    def __init__(self, work):
        self.work   = work

        self.file   = None
        self.ftp    = None

        self.is_smae_size_cnt   = 0
        self.before_work_size   = 0L

    def __is_stopped_work__(self, now_file_size):

        if self.before_work_size == now_file_size:
            self.is_smae_size_cnt += 1

            if self.is_smae_size_cnt > 10:
                raise BkmsError.err_work_same_size('CMD_FTP_SYS_FAIL')

        else:
            self.before_work_size = now_file_size
            self.is_smae_size_cnt = 0

    def __handle_download__(self, block):
        try:
            self.file.write(block)
        except:
            raise BkmsError.err_download_write('CMD_FTP_SYS_FAIL')

        self.work.work_size += len(block)

        if not self.work.event.is_service():
            raise BkmsError.err_serivce_term('CMD_BDM_BKUP_STOP')

    def __handle_upload__(self, blocksize):
        try:
            block = self.file.read(blocksize)
        except:
            raise BkmsError.err_upload_read('CMD_FTP_SYS_FAIL')

        self.work.work_size += len(block)

        if not self.work.event.is_service():
            raise BkmsError.err_serivce_term('CMD_BDM_BKUP_STOP')

        if self.work.event.is_event():
            time.sleep(0.001 * self.work.event.sleep_time())

        return block

    def tbd_speed_control(self, level):
        try:
            self.ftp.putcmd('SLEEP %s' % level)
        except:
            pass


    def bkms_storbinary(self, cmd, fp, blocksize=8192):
        self.ftp.voidcmd('TYPE I')

        conn = self.ftp.transfercmd(cmd)
        self.file = fp

        while 1:
            buf = self.__handle_upload__(blocksize)
            if not buf: break
            conn.sendall(buf)
        conn.close()

        return self.ftp.voidresp()

    def __move_local_dir__(self, directory):
        try:
            os.chdir(directory)
        except:
            raise BkmsError.err_bkms_ftp('CMD_L_DIR_EXCEPT') 

    def __move_remote_dir__(self, directory):
        try:
            self.ftp.cwd(directory)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.work.L.log(0, 'ERR| move_remote_except except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 
            self.work.L.log(0, 'ERR| move_remote_dir except %s' % directory) 
            raise BkmsError.err_bkms_ftp('CMD_R_DIR_EXCEPT')

    def downFtp(self):
        try:
            self.work.L.log(0, 'INF| BkmsFtp:downFtp thread start')

            self.work.now_file_size = 0L

            self.ftp = ftplib.FTP()
            self.ftp.connect(self.work.ip, int(self.work.f_port))
            self.work.L.log(0, 'INF| BkmsFtp::downFtp connect success')

            self.ftp.login(self.work.user, self.work.pwd)
            self.work.L.log(0, 'INF| BkmsFtp::downFtp login success') 

            self.__move_local_dir__(self.work.target_directory)
            self.work.L.log(0, 'INF| BkmsFtp::downFtp local dir %s' % \
                    self.work.target_directory) 

            self.__move_remote_dir__(self.work.source_base_position)
            self.work.L.log(0, 'INF| BkmsFtp::downFtp remote dir %s' % \
                    self.work.source_base_position) 

        except BkmsError.Error, strerror:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.work.L.log(0, 'ERR| BkmsFtp::downFtp except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 
            self.work.event.set_terminate(False, strerror) 
            try: self.ftp.quit()
            except: pass

            return
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.work.L.log(0, 'ERR| BkmsFtp::downFtp except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 
            self.work.event.set_terminate(False, 'CMD_FTP_SYS_FAIL') 
            try: self.ftp.quit()
            except: pass

            return


        for directory in self.work.source_directory_list:
            try:
                os.makedirs(directory)
                self.work.L.log(1, 
                        'INF| BkmsFtp::downFtp make dir:%s' % directory)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.work.L.log(0, 'ERR| BkmsFtp::downFtp except : %s' \
                    % repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback))) 
                self.work.event.set_terminate(False, 'CMD_L_DIR_EXCEPT') 
                try: self.ftp.quit()
                except: pass

                return

        # f is file_name + file_mode : example.txt755
        for f in self.work.source_file_list:
            file_name = f[:-3]
            self.work.L.log(0, 
                'INF| BkmsFtp::down try to retr %s mode %s' % \
                 (file_name, f[-3:]))

            try:
                self.file = open(file_name, "wb")
                self.ftp.retrbinary('RETR ' + file_name, 
                        self.__handle_download__, 262144)
                self.file.close()

                self.__chmod__(f)

            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.work.L.log(0, 'ERR| BkmsFtp::downFtp except : %s' \
                    % repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback))) 
                self.work.event.set_terminate(False, 'CMD_FTP_SYS_FAIL') 

                try: file.close()
                except: pass

                try: self.ftp.abort()
                except: pass

                try: self.ftp.quit()
                except: pass

                return

        self.work.event.set_terminate(True, '') 
        self.work.L.log(0, 'INF| BkmsFtp::downFtp success')
        self.work.L.log(0, 'INF| BkmsFtp::downFtp thread terminate')

        try: self.ftp.quit()
        except: pass

        return

    def __chmod__(self, file_name_and_mode):
        file_name = file_name_and_mode[:-3]
        mode = file_name_and_mode[-3:]

        if mode == 'FFF':
            return

        apply_mode = int(mode[0]) << 6
        apply_mode += int(mode[1]) << 3
        apply_mode += int(mode[2])
       
        os.chmod(file_name, apply_mode)

    def upFtp(self):
        try:
            self.work.L.log(0, 'INF| BkmsFtp::upFtp thread start')

            self.work.now_file_size = 0L

            self.ftp = ftplib.FTP()
            self.ftp.connect(self.work.ip, int(self.work.f_port))
            self.work.L.log(0, 'INF| BkmsFtp::upFtp connect success')

            self.ftp.login(self.work.user, self.work.pwd)
            self.work.L.log(0, 'INF| BkmsFtp::upFtp login success') 

            self.__move_local_dir__(self.work.source_base_position)
            self.work.L.log(0, 'INF| BkmsFtp::upFtp local dir %s' % \
                    self.work.source_base_position) 

            self.__move_remote_dir__(self.work.target_directory)
            self.work.L.log(0, 'INF| BkmsFtp::upFtp remote dir %s' % \
                    self.work.target_directory) 
        except BkmsError.Error, strerror:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.work.L.log(0, 'ERR| BkmsFtp::upFtp except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 
            self.work.event.set_terminate(False, strerror) 
            try: self.ftp.quit()
            except: pass

            return

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.work.L.log(0, 'ERR| BkmsFtp::upFtp except : %s' \
                % repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback))) 
            self.work.event.set_terminate(False, 'CMD_FTP_SYS_FAIL') 
            try: self.ftp.quit()
            except: pass

            return

        for directory in self.work.source_directory_list: 
            try:
                self.ftp.mkd(directory)
                self.work.L.log(1, 
                        'INF| BkmsFtp::upFtp make dir:%s' % directory)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.work.L.log(0, 'ERR| BkmsFtp::upFtp except : %s' \
                    % repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback))) 
                self.work.event.set_terminate(False, 'CMD_R_DIR_EXCEPT') 
                try: self.ftp.quit()
                except: pass

                return

        for f in self.work.source_file_list:

            file_name = f[:-3]
            self.work.L.log(0, 
                'INF| BkmsFtp::upFtp try to store ## %s' % file_name)

            try:
                file = open(file_name, 'rb')
                self.bkms_storbinary('STOR ' + file_name, file, 262144)
                file.close()

            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.work.L.log(0, 'ERR| BkmsFtp::upFtp except : %s' \
                    % repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback))) 

                self.work.event.set_terminate(False, 'CMD_FTP_SYS_FAIL') 

                try: file.close()
                except: pass

                try: self.ftp.abort()
                except: pass

                try: self.ftp.quit()
                except: pass

                return

        self.work.event.set_terminate(True, '') 
        self.work.L.log(0, 'INF| BkmsFtp::upFtp success')
        self.work.L.log(0, 'INF| BkmsFtp::upFtp thread terminate')

        try: self.ftp.quit()
        except: pass

        return

