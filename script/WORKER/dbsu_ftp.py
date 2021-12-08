
import sys
import time

import CommLog
import BkmsService

def valid_arg(argv):
    '''
    dbsu_ftp.py [FTP|TBD]   [SYS_NAME] [schd_code] [T_IP]   [T_PORT] 
                [USER]      [PWD]      [BKUP]      [SOURCE] [DBM_IP] 
                [DBM_PORT]  [SOURCE]   [BASE_BACKUP_POS]
    dbsu_ftp.py [FTP|TBD]   [SYS_NAME] [schd_code] [T_IP]   [T_PORT] 
                [USER]      [PWD]      [DUMP]      [SOURCE] [DBM_IP] 
                [DBM_PORT]  [TARGET]   [BASE_BACKUP_POS]
    '''
    
    if len(argv) != 14:
        return False

    if argv[8] != 'DUMP' and argv[8] != 'BKUP':
        return False

    return True

def start_banner(L):
    L.log(0, '')
    L.log(0, '==========================================================')
    L.log(0, 'dbsu_ftp START')
    L.log(0, '==========================================================')

def end_banner(L):
    L.log(0, 'dbsu_ftp END')
    L.log(0, '==========================================================')

if __name__ ==  '__main__':

    L = CommLog.Log(3)

    start_banner(L)

    if not valid_arg(sys.argv):
        L.log(0, 'main::argument  error :%s' % sys.argv)
        end_banner(L)
        sys.exit()
    
    s = BkmsService.BkmsService(L)

    if not s.init(sys.argv):
        L.log(0, 'main::get_information error :%s' % sys.argv)
        end_banner(L)
        sys.exit()

    s.service()
    time.sleep(5)
    s.clear()
    end_banner(L)
    sys.exit()

