
import sys 

import BkmsError

_MSG_CMD = {
    'CMD_BDM_BKUP_REQ':'3001',
    'CMD_BDM_BKUP':'3002',
    'CMD_BDM_DUMP_REQ':'3003',
    'CMD_BDM_DUMP':'3004',
    'CMD_BDM_BKUP_STOP':'3005',
    'CMD_BDM_BKUP_CONTINUE':'3006',
    'CMD_BDM_BKUP_REST':'3007',
    'CMD_BDM_DUMP_STOP':'3008',
    'CMD_BDM_DUMP_CONTINUE':'3009',
    'CMD_BDM_DUMP_REST':'3010',
    'CMD_BDM_BKUP_FTPREQ':'3011',
    'CMD_BDM_DUMP_FTPREQ':'3012',

    'CMD_BKUP_S_NOFILE':'3016',
    'CMD_DUMP_S_NOFILE':'3017',

    'CMD_BKUP_S_NODIR':'3018',
    'CMD_DUMP_S_NODIR':'3019',

    'CMD_R_MAKE_DIR':'3020',
    'CMD_L_DIR_EXCEPT':'3021',
    'CMD_R_DIR_EXCEPT':'3022',
    'CMD_LOGIN_FAIL':'3023',
    'CMD_CONN_FAIL':'3024',
    'CMD_FTP_SYS_FAIL':'3025',
    'CMD_TBD_UTIL_FAIL':'3026',

    '3005':'3005',
    '3008':'3008',

    'null':None,
}

_MSG_LEN = {
    'HEADER':26,
    'BODY':529,
    'ALLOW_BODY':14,
    'ALLOW':'320',
}

_DB_MACRO = {
    'DB_IP':'127.0.0.1',
    'DB_PORT':3306,
    'DB_USER':'dbsu',
    'DB_PWD':'dbsu',
    'DB_NAME':'dbsu',
    'null': None,
}

_BKMS_MACRO = {
    'FTP_PORT':'21',
    'TBD_PORT':'24575',
    'null':None,
}

def M_DB(tag):
    try:
        return _DB_MACRO[tag]
    except KeyError:
        return ''

def M_BKMS(tag):
    try:
        return _BKMS_MACRO[tag]
    except KeyError:
        return ''

class BkmsMessage(object):
    def __init__(self):
        self.header = ''
        self.body = ''

        self.parse_header = {}
        self.parse_body   = {}

    def get_message(self):
        return self.header + self.body

    def enc_header(self, cmd, source, target, msglen, resp):
        self.header = ''
        self.header += cmd.zfill(4)
        self.header += source.zfill(4)
        self.header += target.zfill(4)
        self.header += str(msglen).zfill(10)
        self.header += resp.zfill(4)

    def enc_body(self, host_name, command, base_path, backup_path, file_name, 
        file_mode, total_file_count, now_file_count, data_count, now_file_size,
        total_file_size, end_flag,
        cpu_usage, mem_usage, net_usage, disk_usage, msg_len):

        self.body = \
        '%-20s%-100s%-100s%-100s%-80s%-4s%-12s%-12s%-12s%-32s%-32s%-1s%-3s%-3s%-3s%-3s%-12s' % (host_name, command, base_path, backup_path, file_name,
file_mode, total_file_count,now_file_count, data_count, now_file_size,
total_file_size, end_flag, cpu_usage.zfill(3), mem_usage.zfill(3),
net_usage.zfill(3), disk_usage.zfill(3), msg_len)

    def enc_allow_body(self,
        sys_index, t_sys_index, date, backup_path, restore_path, bkms_path):

        self.body = '%-6s%-6s%-8s%-100s%-100s%-100s' % \
            (sys_index.zfill(6), t_sys_index.zfill(6),
                date, backup_path, restore_path, bkms_path)

    def get_header(self, value):
        try:
            return self.parse_header[value]
        except KeyError:
            return ''

    def get_body(self, value):
        try:
            return self.parse_body[value]
        except KeyError:
            return ''

    def dec_header(self, data):
        if len(data) < _MSG_LEN['HEADER']:
            raise BkmsError.err_bkms_message

        try:
            self.parse_header.clear()
            self.parse_header['CMD']    = data[:4].strip()
            self.parse_header['SRC']    = data[4:8].strip()
            self.parse_header['TARGET'] = data[8:12].strip()
            self.parse_header['LEN']    = int(data[12:22].strip())
            self.parse_header['RESP']   = data[22:26].strip() 
        except:
            raise BkmsError.err_bkms_message

    def dec_allow_body(self, data):
        if len(data) < _MSG_LEN['ALLOW_BODY']:
            raise BkmsError.err_bkms_message

        try:
            self.parse_body.clear()

            self.parse_body['WORK_DATE'] = data[0:8].strip()
            self.parse_body['WORK_TIME'] = data[8:14].strip()

            limit_list = data[14:].split()

            self.parse_body['L_CPU'] = limit_list[0]
            self.parse_body['M_CPU'] = limit_list[1]
            self.parse_body['H_CPU'] = limit_list[2]

            self.parse_body['L_MEM'] = limit_list[3]
            self.parse_body['M_MEM'] = limit_list[4]
            self.parse_body['H_MEM'] = limit_list[5]

            self.parse_body['L_DISK'] = limit_list[6]
            self.parse_body['M_DISK'] = limit_list[7]
            self.parse_body['H_DISK'] = limit_list[8]

            self.parse_body['L_NET'] = limit_list[9]
            self.parse_body['M_NET'] = limit_list[10]
            self.parse_body['H_NET'] = limit_list[11]
        except:
            raise BkmsError.err_bkms_message

    def dec_body(self, data):
        if len(data) < _MSG_LEN['BODY']:
            raise BkmsError.err_bkms_message

        try:
            self.parse_body.clear()

            self.parse_body['HOST_NAME'] = data[0:20].strip()
            self.parse_body['COMMAND'] = data[20:120].strip()
            self.parse_body['BASE_PATH'] = data[120:220].strip()
            self.parse_body['BACKUP_PATH'] = data[220:320].strip()
            self.parse_body['FILE_NAME'] = data[320:400].strip()
            self.parse_body['FILE_MODE'] = data[400:404].strip()
            self.parse_body['TOTAL_FILE_COUNT'] = data[404:416].strip()
            self.parse_body['NOW_FILE_COUNT'] = data[416:428].strip()
            self.parse_body['DATA_COUNT'] = data[428:440].strip()
            self.parse_body['NOW_FILE_SIZE'] = data[440:472].strip()
            self.parse_body['TOTAL_FILE_SIZE'] = data[472:504].strip()
            self.parse_body['END_FLAG'] = data[504:505].strip()
            self.parse_body['CPU_USAGE'] = data[505:508].strip()
            self.parse_body['MEM_USAGE'] = data[508:511].strip()
            self.parse_body['NET_USAGE'] = data[511:514].strip()
            self.parse_body['DISK_USAGE'] = data[514:517].strip()
            self.parse_body['MSG_LEN'] = data[517:529].strip()
        except:
            raise BkmsError.err_bkms_message
