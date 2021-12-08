
import re 
import sys
import time
import traceback

from itertools import *

import BkmsError

class LINUXSystem(object):
    def __init__(self, iLog):
        self.L          = iLog
        self.is_cpu_first   = True
        self.is_mem_first   = True
        self.is_disk_first  = True
        self.is_net_first   = True

        self.cpu_used = 0
        self.cpu_idle = 0

        self.total_mem_size = ''

        self.net_flag   = True
        self.net_in = 0L
        self.net_out= 0L

        self.L.log(0, 'INF| RemoteSystem::Linux System init')

    def get_cpu(self, itelnet):
        time.sleep(3)

        cpu_used = 0L
        cpu_idle = 0L

        used_rate = '0'

        try:
            ret = itelnet.execute_command('cat /proc/stat', 10)
            for line in ret.split('\n'):
                line = line.strip()
                if len(line.split()) < 5:
                    continue

                if line.split()[0] == 'cpu':
                    cpu_used = long(line.split()[1]) + long(line.split()[2]) + long(line.split()[3]) 
                    cpu_idle = long(line.split()[4])
                    self.L.log(2, '## INF|get_cpu used: %s idle:%s' % (cpu_used, cpu_idle)) 
                    break

            if self.cpu_used == 0 and self.cpu_idle == 0:
                self.L.log(2, '### INF|get_cpu used: 0 idle:0') 
                self.cpu_used = cpu_used
                self.cpu_idle = cpu_idle
            else:
                temp_used = cpu_used - self.cpu_used
                temp_idle = cpu_idle - self.cpu_idle
                self.cpu_used = cpu_used
                self.cpu_idle = cpu_idle

                if temp_used < 0 or temp_idle:
                    self.L.log(2, '## INF|get_cpu temp_used: %s temp_idle:%s' % \
                        (temp_used, temp_idle)) 
                    used_rate = '0'
                else: 
                    used_rate = str(1.0 * temp_used / (temp_used + temp_idle) * 100) 

        except :
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(2, '## WRN|get_cpu except: %s' \
                % repr(traceback.format_exception(exc_type, exc_value, exc_traceback))) 
            used_rate = '0'

        return used_rate
       
    def get_mem(self, itelnet):
        time.sleep(3)

        total = 0
        used  = 0 

        used_rate = ''

        try:
            ret = itelnet.execute_command('free -m', 10)
            for line in ret.split('\n'):
                line = line.strip()
                if len(line.split()) < 3:
                    continue
                if 'Mem' in line:
                    total = int(line.split()[1])
                    used  = int(line.split()[2])
                    break

            used_rate = str((1.0 * used / total) * 100).split('.')[0]
        except :
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(2, '## WRN|get_mem except: %s' \
                % repr(traceback.format_exception(exc_type, exc_value, exc_traceback))) 
            used_rate = '0'
           
        return used_rate

    def get_disk(self, itelnet):
        ret = ''
        if self.is_disk_first:
            try:
                ret = itelnet.execute_command('which df', 10)
            except:
                raise BkmsError.err_linux_system

            if 'no df' in ret:
                raise BkmsError.err_linux_system('NO_DF')

            self.is_disk_first = False

        try:
            ret = itelnet.execute_command('df -k', 5)
        except:
            raise BkmsError.err_linux_system

        self.L.log(2, 'get_disk::ret [%s]' % ret)

        if not len(ret):
            raise BkmsError.err_linux_system
       
        l = []

        for i in ifilter(lambda x:'%' in x, ret.split()):
            try:
                val = int(i.replace('%', ''))
                l.append(val)
            except:
                pass

        if len(l) == 0:
            return '0'

        return str(int(1.0 * sum(l)/len(l)))

    def get_network(self, itelnet):
        time.sleep(1)

        ret = ''

        try:
            ret = itelnet.execute_command('cat /proc/net/dev', 5)
            ret = ret.split(itelnet._newline)
            ret = ret[2:]
        except:
            raise BkmsError.err_linux_system('NO_CAT_NET_DEV')

        in_byte  = 0L
        out_byte = 0L

        for line in ret:
            if 'lo' in line:
                continue

            line = line[line.find(':')+1:]
            in_byte = in_byte + long(line.split()[1])
            out_byte= out_byte+  long(line.split()[9])

        if self.net_flag:
            self.net_flag = False
            self.net_in = in_byte
            self.net_out= out_byte
            return '0'

        in_val = abs(in_byte - self.net_in)
        out_val= abs(out_byte- self.net_out)

        self.net_in = in_byte
        self.net_out= out_byte

        return str(int((in_val + out_val) / (1024 * 1024)))


class HPUXSystem(object):
    def __init__(self, iLog):
        self.L          = iLog

        self.is_cpu_first   = True
        self.is_mem_first   = True
        self.is_disk_first  = True
        self.is_net_first   = True

        self.memory_command = ''

        self.total_mem_size = ''

        self.L.log(0, 'INF| RemoteSystem::HPUX System init')

    def get_cpu(self, itelnet):
        ret = ''
        if self.is_cpu_first:
            try:
                ret = itelnet.execute_command_until('~/TBD/bkms_get_cpu 1 36000','CPU', 10)
            except:
                raise BkmsError.err_hpux_system('NO_GET_CPU')

            self.L.log(2, '##### choi get_cpu ret: %s' % ret)

            if not 'CPU' in ret:
                raise BkmsError.err_hpux_system('NO_GET_CPU') 

            self.is_cpu_first = False

        while True:
            ret = itelnet.read_until_newline(10)

            if not ret.split():
                continue
            else:
                pass

            try: 
                val = ret.split().pop()
                val = val.split('.')[0]

                return str(int(val))
            except AttributeError:
                pass
      
    def get_mem(self, itelnet):
        ret = ''
        if self.is_mem_first:
            try:
                ret = itelnet.execute_command_until('~/TBD/bkms_get_mem 1 36000', 'MEM', 10)
            except:
                raise BkmsError.err_hpux_system('NO_MEM') 

            self.L.log(2, '##### choi get_mem ret: %s' % ret)

            if not 'MEM' in ret:
                raise BkmsError.err_hpux_system('NO_MEM') 

            self.is_mem_first = False

        while True:
            try:
                ret = itelnet.read_until_newline(10)
            except:
                raise BkmsError.err_hpux_system('READ_MEM_EXCEPT') 

            if not ret.split():
                continue
            else:
                pass

            try: 
                val = ret.split().pop()
                val = val.split('.')[0]

                return str(int(val))
            except AttributeError:
                pass

    def get_disk(self, itelnet):
        ret = ''
        if self.is_disk_first:
            try:
                ret = itelnet.execute_command('which bdf', 10)
            except:
                raise BkmsError.err_hpux_system('NO_DF')

            if 'no df' in ret:
                raise BkmsError.err_hpux_system('NO_DF')

            self.is_disk_first = False

        try:
            ret = itelnet.execute_command('bdf', 5)
        except:
            raise BkmsError.err_hpux_system('BDF_COMMAND_EXCEPT')
            
        self.L.log(2, 'get_disk::ret [%s]' % ret)

        if not len(ret):
            raise BkmsError.err_hpux_system
       
        l = []

        for i in ifilter(lambda x:'%' in x, ret.split()):
            try:
                val = int(i.replace('%', ''))
                l.append(val)
            except:
                pass

        if len(l) == 0:
            return '0'

        return str(int(1.0 * sum(l)/len(l)))

    def get_network(self, itelnet):
        ret = ''
        try:
            ret = itelnet.execute_command('which netstat', 10)
        except:
            raise BkmsError.err_hpux_system('NETSTAT COMMAND EXCEPT')

        if 'no netstat' in ret:
            raise BkmsError.err_hpux_system('NO NETSTAT')

        try:
            itelnet.execute_command('netstat -ni 1', 10)
        except:
            raise BkmsError.err_hpux_system('NETSTAT COMMAND EXCEPT')

        val = 0

        while True:
            try:
                ret = itelnet.read_until_newline(10)

                in_packet = ret.split()[2]
                out_packet= ret.split()[3]

                val = 1.0 * 1500 * (int(in_packet) + int(out_packet))
                val = int(val / (1024 * 1024)) 

                self.L.log(2, '##### choi get_network ret: %s' % ret)

                if val > 100 or val < 0:
                    continue

                break
            except:
                pass

        return str(val)

class SUNSystem(object):
    def __init__(self, iLog):
        self.L          = iLog
        self.is_cpu_first   = True
        self.is_mem_first   = True
        self.is_disk_first  = True
        self.is_net_first   = True
        self.total_mem_size = ''

        self.L.log(0, 'INF| RemoteSystem::SunSystem init')

    def get_cpu(self, itelnet):
        ret = ''
        
        if self.is_cpu_first:
            try:
                ret=itelnet.execute_command('which sar',10)
            except:
                raise BkmsError.err_sun_system('NO_SAR_COMMAND')
            
            if 'no sar' in ret:
                raise BkmsError.err_sun_system('NO_SAR')

            try:
                itelnet.execute_command_until('sar 1 36000','\n', 10)
            except:
                raise BkmsError.err_sun_system('NO_SAR_COMMAND')

            self.is_cpu_first = False

        while True:
            try:
                ret = itelnet.read_until_newline(10)
            except:
                raise BkmsError.err_sun_system('SAR_RESP_EXCEPT')

            if not ret.split():
                continue
            else:
                pass

            val = ret.split().pop()

            try:
                if re.match('[0-9]+', val).group() == val and int(val) <= 100:
                    return str(100 - int(val))
                else:
                    pass
            except AttributeError:
                pass

    def get_mem(self, itelnet):
        ret = ''
        if self.is_mem_first:
            try:
                ret = itelnet.execute_command('which prtconf',10)
            except:
                raise BkmsError.err_sun_system('NO_PRTCOCNF_COMMAND')

            if 'no prtconf' in ret:
                raise BkmsError.err_sun_system('NO_PRTCOCNF')

            try:
                ret = itelnet.execute_command('prtconf | grep Memory', 10)
            except:
                raise BkmsError.err_sun_system('NO_PRTCOCNF_COMMAND')
                
            self.total_mem_size = re.search('[0-9]+', ret).group()

            self.L.log(0, 'INF| SunSystem::get_mem prtconf ret :%s' % ret)

            if not self.total_mem_size:
                raise BkmsError.err_sun_system

            try:
                itelnet.execute_command_until('vmstat 1 36000', '\n', 10)
            except:
                raise BkmsError.err_sun_system('VMSTAT_COMMAND')
                
            self.is_mem_first = False
   
        while True:
            try:
                ret = itelnet.read_until_newline(10)

                if re.search('[a-z]+', ret):
                    continue

                val = 1 - 1.0 * \
                    int(ret.split()[4])/(long(self.total_mem_size) * 1024)
            except:
                raise BkmsError.err_sun_system('VMSTAT_RESP_EXCEPT')

            if int(val * 100) >= 0 and int(val * 100) <= 100:
                return str(int(val * 100))
            else:
                pass

    def get_disk(self, itelnet):
        ret = ''
        if self.is_disk_first:
            try:
                ret = itelnet.execute_command('which df',10)
            except:
                raise BkmsError.err_sun_system('DF_COMMAND')

            if 'no df' in ret:
                raise BkmsError.err_sun_system('NO_DF')

            self.is_disk_first = False

        try:
            ret = itelnet.execute_command('df -k', 5)
        except:
            raise BkmsError.err_sun_system('DF_COMMAND')

        if not len(ret):
            raise BkmsError.err_sun_system
       
        l = []

        for i in ifilter(lambda x:'%' in x, ret.split()):
            try:
                val = int(i.replace('%', ''))
                l.append(val)
            except:
                pass

        if len(l) == 0:
            return '0'

        return str(int(1.0 * sum(l)/len(l)))


    def get_network(self, itelnet):
        ret = ''
        if self.is_net_first:
            try:
                ret = itelnet.execute_command('which netstat',10)
            except:
                raise BkmsError.err_sun_system('NETSTAT COMMAND')

            if 'no netstat' in ret:
                raise BkmsError.err_sun_system('NO NETSTAT')

            try:
                ret = itelnet.execute_command('netstat -ni 1 36000',10)
                ret = itelnet.read_until_newline(10)
                ret = itelnet.read_until_newline(10)
            except:
                raise BkmsError.err_sun_system('NETSTAT COMMAND')
            
            self.is_net_first = False

        try:
            ret = itelnet.read_until_newline(10)
        except:
            raise BkmsError.err_sun_system('NETSTAT_RESP_EXCEPT')


        in_packet = ret.split()[5]
        out_packet= ret.split()[7]

        val = 1.0 * 1500 * (int(in_packet) + int(out_packet))
        val = int(val / (1024 * 1024)) 
        
        return str(val)


class System(object):
    def __init__(self):
        pass

    def create_instance(self, itelnet, iLog):
        ret = ''
        try:
            ret = itelnet.execute_command('uname', 10)
        except:
            raise BkmsError.err_remote_system('UNAME_COMMAND')

        iLog.log(1, 'INF| System::create_instance ret : %s' % ret)

        if 'Sun' in ret:
            return SUNSystem(iLog)
        elif 'HP' in ret:
            return HPUXSystem(iLog) 
        elif 'Linux' in ret:
            return LINUXSystem(iLog)
        elif 'nonstopux' in ret:
            raise BkmsError.err_not_implements
        else:
            raise BkmsError.err_remote_system 
