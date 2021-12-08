
import sys
import time

from WORKER import CommLog
import BkmsService

def valid_arg(argv):

    # worker.py [ITEM_ID] [REG_TIME] [SYSTEM_NAME]

    '''
    worker.py [FTP|TBD]   [SYS_NAME] [schd_code] [T_IP]   [T_PORT] 
                [USER]      [PWD]      [BKUP]      [SOURCE] [DBM_IP] 
                [DBM_PORT]  [SOURCE]   [BASE_BACKUP_POS]
    worker.py [FTP|TBD]   [SYS_NAME] [schd_code] [T_IP]   [T_PORT] 
                [USER]      [PWD]      [DUMP]      [SOURCE] [DBM_IP] 
                [DBM_PORT]  [TARGET]   [BASE_BACKUP_POS]
    '''
    
    if len(argv) != 4:
        return False

    return True

def start_banner(L):
    L.log(0, '')
    L.log(0, '==========================================================')
    L.log(0, 'worker START')
    L.log(0, '==========================================================')

def end_banner(L):
    L.log(0, 'worker END')
    L.log(0, '==========================================================')

if __name__ ==  '__main__':

    L = CommLog.Log(3)
    L.init('work_%s_%s.log' % (sys.argv[1], sys.argv[3]))

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

