# coding: utf-8

import os
import time
import sys 
import traceback

from itertools import *

import BkmsError

class BkmsDir(object):

    def __init__(self, iLog):
        self.L = iLog

        self._total_size = 0L
        self._dir_list   = []
        self._file_list  = [] 

        self._base_dir   = '' 

    def clear(self):
        self._total_size = 0L
        self._dir_list  = []
        self._file_list = []

    # UP
    def find_local_files(self, dirname):

        if dirname[-1:] == os.sep:
            dirname = dirname[:-1]

        self._base_dir = dirname

        self.L.log(2, 'INF| find_local_files::base_dir : %s' % self._base_dir)

        try:
            if os.path.isfile(dirname):
                mode = self.get_mode_to_number(dirname)
                self._file_list.append(dirname[dirname.rfind(os.sep)+1:]+mode)
                self._total_size = os.stat(dirname)[6]
                self._base_dir = dirname[:dirname.rfind(os.sep)]
                return (self._base_dir, 
                        self._dir_list, 
                        self._file_list, 
                        self._total_size)

            elif os.path.isdir(dirname):
                for now_dir, dirs, files in os.walk(dirname):
                    if not now_dir in self._dir_list and dirname != now_dir:
                        self._dir_list.append( \
                            now_dir.replace(dirname + os.sep, ''))

                    for file in files:
                         
                        if now_dir == dirname:
                            full_file_name = os.path.join(self._base_dir, file)
                            mode = self.get_mode_to_number(full_file_name) 
                            self._total_size += os.stat(full_file_name)[6]

                            self.L.log(0, 
                                'INF| BkmsDir::find_local_files file:%s size:%s mode %s' % (full_file_name, os.stat(full_file_name)[6], mode))  
                            
                            self._file_list.append(file+mode)
                        else:
                            full_file_name = \
                                os.path.join(self._base_dir, 
                                     os.path.join(now_dir.replace(dirname + os.sep, ''),file))
                            mode = self.get_mode_to_number(full_file_name)
                            self._total_size += os.stat(full_file_name)[6]

                            self.L.log(0, 
                                'INF| BkmsDir::find_local_files file:%s size:%s mode %s' % (full_file_name, os.stat(full_file_name)[6], mode))  

                            self._file_list.append(os.path.join( \
                                now_dir.replace(dirname + os.sep, ''),file)+mode)
            else:
                raise BkmsError.err_bkms_dir('find local file fail : local position is not regular file/dir')

            '''
            for f in self._file_list:
                self._total_size += os.stat(os.path.join(self._base_dir, f))[6]

                self.L.log(0, 
                    'INF| BkmsDir::find_local_files file:%s size:%s' % \
                        (f, os.stat(os.path.join(self._base_dir, f))[6]))
           '''

        except:
            raise BkmsError.err_bkms_dir('find local file fail : exception')



        return (self._base_dir, 
                self._dir_list, 
                self._file_list, 
                self._total_size)


    # DOWN ver2 
    def find_remote_files(self, itelnet, user_name, source):
        ret = ''
        if source[-1:] == os.sep:
            source = source[:-1]

        '''
        if not self.__is_writable__(itelnet, source[:source.rfind(os.sep)]):
            raise BkmsError.err_bkms_dir('CMD_R_DIR_EXCEPT')
        '''

        self._base_dir = source

        if '*' in self._base_dir:
            d = self._base_dir[:self._base_dir.find('*')]
            self._base_dir = d[:d.rfind(os.sep)]

        try:
            ret = itelnet.execute_command('groups %s' % user_name, 5)
            groups = ret.split(itelnet._newline)[0].split()

            #self._base_dir = self.change_dir_for_link(itelnet, self._base_dir)
            ret = itelnet.execute_command('find %s -type d' % self._base_dir, 20)
            ret = ret.split(itelnet._newline)

            self.L.log(2, 'INF| source dir ret : %s' % ret)

            if len(ret) == 1 and 'cannot' in ret:
                raise BkmsError.err_bkms_dir('find remote file : base dir is not exist')

            for sub_dir in ret:
                sub_dir = sub_dir.strip()

                if not sub_dir:
                    break

                if sub_dir == self._base_dir:
                    continue

                self._dir_list.append(
                    sub_dir.replace(self._base_dir+os.sep,''))
           
            try:
                if '*' in source:
                    ret = itelnet.execute_command('find %s -type f -exec ls -l {} \;' %  \
                        source, 20)
                else:
                    ret = itelnet.execute_command('find %s -type f -exec ls -l {} \;' %  \
                        self._base_dir, 20)
                ret = ret.split(itelnet._newline)
            except:
                raise BkmsError.err_bkms_dir('find remote file fail : find command fail')

            if len(ret) == 0:
                raise BkmsError.err_bkms_dir('find remote file : no data')

            for line in ret:
                line = line.strip()

                if len(line.split()) < 5:
                    continue

                value = line.split()

                perm  = value[0]
                user  = value[2]
                group = value[3]
                size  = value[4]
                name  = value[-1:][0] 
                        
                self.L.log(0, 
                   'INF| BkmsDir::find_remote_files perm %s file:%s size:%s'\
                    % (perm, name, size))

                if name == self._base_dir:
                    self._base_dir = name[:name.rfind(os.sep)]

                if perm[0] != '-':
                    continue

                mode_num = self.chg_mode_to_number(perm)

                if perm[7] == 'r':
                    self._file_list.append(
                            name.replace(self._base_dir+os.sep,'') + mode_num)
                    self._total_size += long(size)
                    self.L.log(0, 
                        'INF| BkmsDir::find_remote_files file:%s size:%s' % \
                        (name, size))
                    continue

                if perm[4] == 'r':
                    if group in groups:
                        self._file_list.append(
                            name.replace(self._base_dir+os.sep,'') + mode_num)
                        self._total_size += long(size)
                        self.L.log(0, 
                           'INF| BkmsDir::find_remote_files file:%s size:%s'\
                            % (name, size))
                    else:
                        pass

                    continue

                if perm[1] == 'r':
                    if user == user_name:
                        self._file_list.append(
                            name.replace(self._base_dir+os.sep,'') + mode_num)
                        self._total_size += long(size)
                        self.L.log(0, 
                            'INF| BkmsDir::find_remote_files file:%s size:%s'\
                            % (name, size))
                    else:
                        pass

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
                "WRN| BkmsDir::find_remote_files fail :%s" \
                 % repr(traceback.format_exception( exc_value, exc_value,  
                                                    exc_traceback)))
            raise BkmsError.err_bkms_dir('find remote fail : exception')

        return (self._base_dir, 
                self._dir_list, 
                self._file_list, 
                self._total_size)

    # UP
    def find_remote_target_position(self, itelnet, dirname):
        r_dir = dirname

        if r_dir[-1:] == os.sep:
            r_dir = r_dir[:-1]

        real_target_dir = '' 

        real_target_dir = r_dir[:r_dir.rfind(os.sep)]

        '''
        if not itelnet.is_dir(real_target_dir):
            raise BkmsError.err_bkms_dir('CMD_DUMP_S_NODIR')
        '''

        real_target_dir = os.path.join(real_target_dir,
            time.strftime('%Y%m%d%H%M%S') + '_' + \
            r_dir[r_dir.rfind(os.sep)+1:])

        self.L.log(0, 'INF| BkmsDir::find_remote_target_position %s' % \
            real_target_dir) 

        try:
            itelnet.make_remote_dir(real_target_dir)
        except:
            raise BkmsError.err_bkms_dir('make remote dir : exception')

        return real_target_dir

    # DOWN
    def find_local_target_position(self, work):
        real_local_dir  = ''

        target      = work.local_dir
        #sys_name    = work.system
        #source      = work.source 
        source      = work.source_base_position

        #real_local_dir = work.backup_base_pos 
        real_local_dir = os.sep
        self.L.log(3, 
            'DEG| BkmsDir::find_local_target_position target1 [%s]' % target) 

        # 2013.08.16 

        try:
            target = os.path.join(target, 
                                  work.work_date,
                                  work.system,
                                  work.work_time)

            for meta_dir in ifilter(lambda x:x, target.split(os.sep)):
                real_local_dir = os.path.join(real_local_dir, meta_dir)
                self.__make_dir__(real_local_dir)

        except:
            raise BkmsError.err_bkms_dir('Failed to create a local directory')

        if '*' in real_local_dir:
            d = real_local_dir[:real_local_dir.find('*')]
            real_local_dir = d[:d.rfind(os.sep)] + os.sep

        work.real_local_dir = real_local_dir
        return real_local_dir 


    def remove_dir(self, itelnet, dirname):

        try:
            rm_command = ''
            ret = itelnet.execute_command('which rm', 5)
            ret_list = ret.split()
            for i in ret_list:
                if 'rm' in i and os.sep in i:
                    rm_command = i.strip()
                    break

            self.L.log(0, 'INF| BkmsDir::remove_dir which rm ret:%s' % ret)
            
            if len(ret) == 1 and 'no' in ret:
                self.L.log(0, 'WRN| BkmsDir::remove_dir which rm return : %s' % ret)
                return False 

            ret = itelnet.execute_command('%s -rf %s' % (rm_command, dirname), 5)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
                "WRN| BkmsDir::remove_dir fail :%s" \
                 % repr(traceback.format_exception( exc_value, exc_value,  
                                                    exc_traceback)))
            return False

        self.L.log(0, 'INF| BkmsDir::remove_dir rm : %s ret: %s' % (dirname,ret))
        return True


    def __make_dir__(self, dirname):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        elif os.path.isfile(dirname):
            os.unlink(dirname)
            os.mkdir(dirname)
        else:
            pass

    def __is_writable__(self, itelnet, dirname):
        ret = ''

        if dirname[-1:] == os.sep: 
            test_file = dirname[:-1]

        test_file = os.path.join(dirname, 'bkms_test.txt')


        try:
            ret = itelnet.execute_command("echo 'write test' >> %s" \
                    % test_file, 10)
        except:
            raise BkmsError.err_bkms_dir('can not write : command exception')

        if test_file in ret:
            self.L.log(0, 'ERR| BkmsDir::is_writable dir fail : %s, %s' % \
                    (dirname, test_file))
            raise BkmsError.err_bkms_dir('can not write')
        
        return self.remove_dir(itelnet, test_file) 

    def __mode_to_number__(self, mode):
        n = 0

        if 'r' in mode:
            n += 4

        if 'w' in mode:
            n += 2

        if 'x' in mode:
            n += 1

        return str(n)

    def chg_mode_to_number(self, mode):
        if mode[0] != '-':
            return 'FFF'

        temp = ''

        for c in mode[1:]:
            if c == '-' or c == 'r' or c == 'w' or c == 'x':
                temp += c

        first = self.__mode_to_number__(temp[0:3])
        second= self.__mode_to_number__(temp[3:6])
        third = self.__mode_to_number__(temp[6:9])

        return first + second + third

    def get_mode_to_number(self, file_name):

        mode = os.stat(file_name).st_mode

        first = (mode & 448) >> 6
        second = (mode & 56) >> 3
        third  = (mode & 7)     
                                
        return str(first) + str(second) + str(third)

    def remote_chmod(self, itelnet, file_name, mode):
        ret = ''

        try:
            ret = itelnet.execute_command("chmod %s %s" % (mode, file_name), 10)
        except:
            raise BkmsError.err_bkms_dir('remote chmod command fail')

        if 'chmod' in ret:
            self.L.log(0, 'ERR| BkmsDir::remote_chmod fail : %s, %s' % \
                    (file_name, mode))
            raise BkmsError.err_bkms_dir('remote chmod command fail')

        self.L.log(0, 
            'INF| BkmsDir::remote_chmodr %s %s' % (file_name,mode))
 
    def change_dir(self, itelnet, directory):
        ret = ''

        try:
            ret = itelnet.execute_command("cd %s" % directory, 10)
        except:
            raise BkmsError.err_bkms_dir('change the remote dir fail : cd command')

        self.L.log(0, 
            'INF| BkmsDir::change_dir %s ' % directory)

    def change_dir_for_env_variable(self, itelnet, directory):
        ret = ''
        temp_dir = directory

        name_list = temp_dir.split(os.sep)
        var_list = []
        for var in name_list:
            if len(var) > 0 and var[0] == '$':
                var_list.append(var)

        try:
            for var in var_list:
                ret = itelnet.execute_command("echo %s" % var)
                temp_dir = temp_dir.replace(var, ret)
            temp_dir = temp_dir.replace('//', '/')
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
                'INF| BkmsDir::change_dir_for_env_variable except:%s ' % \
                        repr(traceback.format_exception( exc_value, exc_value,
                                                         exc_traceback)))
            raise BkmsError.err_bkms_dir('change the remote dir : for env varialabe')

        return temp_dir

    def abs_dir_for_tilde(self, itelnet, directory):
        ret = ''

        if directory[0:2] != '~/':
            return directory

        try:
            ret = itelnet.execute_command("echo $HOME", 10)
        except:
            raise BkmsError.err_bkms_dir('can not check remote $HOME')

        self.L.log(0, 
            'INF| BkmsDir::abs_dir_for_tilde %s ' % ret.split()[0])

        return ret.split()[0] + directory[1:]

    def change_dir_for_link(self, itelnet, directory):
        ret = ''
        chg_dir = directory

        if chg_dir[-1:] == os.sep:
            chg_dir = chg_dir[:-1]

        try:
            ret = itelnet.execute_command('ls -al %s' % chg_dir, 4)
        except:
            raise BkmsError.err_bkms_dir('change the remote dir fail : link -> real')

        self.L.log(2, 'INF| link check %s %s' % (ret, len(ret)))

        if len(ret) > 0 and (ret.lstrip()[0] == 'l' or ret.lstrip()[0] == '0'):
            link_path = ret.split()[-3]
            real_value = ret.split()[-1]

            if link_path[-1:] == '@':
                link_path = link_path[:-1]

            if real_value[0] == os.sep:
                chg_dir = real_value
            else:
                if not './' in real_value:
                    chg_dir = \
                        chg_dir[:chg_dir.rfind(os.sep)] + os.sep + real_value
                else:
                    chg_dir = \
                        chg_dir[:chg_dir.rfind(os.sep)]
                    
                    for v in real_value.split(os.sep):
                        if v == '..': 
                            chg_dir = \
                                chg_dir[:chg_dir.rfind(os.sep)]
                        else:
                            chg_dir = \
                                chg_dir + os.sep + v
                 
            self.L.log(2, 'INF| change base dir by link %s to %s' % \
                (directory, chg_dir))      

        return chg_dir

