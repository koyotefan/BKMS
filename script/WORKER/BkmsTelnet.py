
import telnetlib
import os 
import sys
import traceback
import time
import struct

import BkmsError

class BkmsTelnet(telnetlib.Telnet):
    def __init__(self, iLog, host=None, port=23, timeout=10, newline='CRLF'):
        self.L      = iLog 
        port = port == '' and 23 or int(port)
        telnetlib.Telnet.__init__(self, host, port)
        self._timeout = timeout
        self.set_newline(newline)
        self.set_option_negotiation_callback(self._negotiate_echo_on)
        self.sock.send(telnetlib.IAC + telnetlib.WILL + telnetlib.NAMS)

    def set_timeout(self, timeout):
        self._timeout = timeout
        self.L.log(1, 'INF| BkmsTelnet::set timeout %s' % timeout)
        return self._timeout


    def set_newline(self, newline):
        old = getattr(self, '_newline', 'CRLF')
        self._newline = newline.upper().replace('LF','\n').replace('CR','\r')
        self.L.log(1, 'INF| BkmsTelnet::set newline %s' % repr(self._newline))
        return old

    def close_connection(self):
        telnetlib.Telnet.close(self)
        ret = self.read_all()
        self.L.log(0, 'INF| BkmsTelnet::close connection %s' % ret)
        return ret

    def login(self, username, password, login_prompt='ogin:',
              password_prompt='assword:'):
        ret = self.read_until(login_prompt, self._timeout)
        if not ret.endswith(login_prompt):
            self.L.log(0, 
                'ERR| BkmsTelnet::login read_until login prompt [%s]' % ret)
            raise BkmsError.err_login('login fail : not found login prompt')

        self.write(username + self._newline)
        ret += self.read_until(password_prompt, self._timeout)
        if not ret.endswith(password_prompt):
            self.L.log(0, 
                'ERR| BkmsTelnet::login read_until password prompt [%s]' % ret)
            raise BkmsError.err_login('login fail : not found password prompt')

        self.write(password + self._newline)

        ret += self._verify_login(ret)
        self._is_complete_login()
        self.get_prompt()
        return ret

    def _verify_login(self, ret):
        time.sleep(1)
        while True:
            try:
                temp = self.read_until('\n', 3)

                if 'set terminal' in temp:
                    self.write('vt100' + self._newline)
                    continue

                ret += temp
                self.L.log(2, 'INF| BkmsTelnet::verify login:[%s]' % temp)
                if len(temp) == 0:
                    return ret
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsTelnet::verify_login except:%s' \
                    % repr(traceback.format_exception( exc_value, exc_value,
                            exc_traceback)))
                raise BkmsError.err_login('login fail : unknown reason')

            if 'incorrect' in ret:
                self.L.log(0, 'WRN| BkmsTelnet::verify_login login fail [%s]' % ret)
                raise BkmsError.err_login('login fail : incorrect')

    def _is_complete_login(self):

        while True:
            self.write(self._newline)
            first = self.read_until('\n',3)

            self.write(self._newline)
            second = self.read_until('\n', 3)

            self.L.log(2, 'INF| BkmsTelnet::is_complete_login F-%s, S-%s' \
                    % (first, second))

            if 'incorrect' in (first, second):
                raise BkmsError.err_login('login fail : incorrect')
            
            if len(first) == 0 and len(second) == 0:
                self.L.log(2, 'INF| BkmsTelnet::is_complete_login False')
                raise BkmsError.err_login('login fail : not complete')

            if len(first.strip()) == 0 or len(second.strip()) == 0:
                continue

            if first == second:
                return

    def get_prompt(self, timeout=None):
        self.write('id' + self._newline)
        ret = ''
        if not timeout:
            timeout = self._timeout
        starttime = time.time()
        while time.time() - starttime < timeout:
            temp = self.read_until(self._newline, 1)
            ret += temp.strip()
            self.L.log(2, 'INF| BkmsTelnet::get_prompt ret:[%s]'% ret)

            if 'uid' in ret:
                p = ret[-1]
                if '$' == p or '%' == p or ']' == p or '>' == p or '#' == p:
                    self._prompt = p
                    self.L.log(2, 'INF| BkmsTelnet::get_prompt prompt:[%s]' \
                         % self._prompt)
                    return self._prompt

        self.L.log(0, 
           'ERR| BkmsTelnet::get_prompt cannot find prompt timeout [%s]' % ret)
        raise BkmsError.err_login('login fail : not found prompt')

    def execute_command(self, text, timeout=None):
        text = str(text)
        self.L.log(2, 'INF| BkmsTelnet::execute_command [%s]'% text)
        self.write(text + self._newline)
        ret = self.read_until_prompt(text, timeout)
        self.L.log(2, 'INF| BkmsTelnet::execute_command resp [%s]'% ret)
        return ret

    def execute_command_until(self, text, expect, timeout=None):

        if not timeout:
            timeout = self._timeout

        text = str(text)
        self.L.log(2, 'INF| BkmsTelnet::execute_command_until [%s]'% text)
        self.write(text + self._newline)
        ret = self.read_until(expect, timeout)
        self.L.log(2, 'INF| BkmsTelnet::execute_command_until resp [%s]'% ret)

        if not ret.endswith(expect):
            self.L.log(2, 
                'ERR| BkmsTelnet::execute_command_until timeout [%s]'% ret)
            raise AssertionError('execute_command_until timeout')

        return ret

    def read_until_prompt(self, command, timeout=None):
        ret = ''
        starttime = time.time()

        if not timeout:
            timeout = self._timeout

        while time.time() - starttime < timeout:
            temp = self.read_until(self._prompt)
            temp += self.read_lazy().strip()
            #print 'read_until_prompt temp:%s' % temp
            if len(ret) == 0 and  not command.split()[0] in temp:
                #print 'inside temp:%s, data:%s' % (temp, ret)
                continue

            ret += temp
            self.L.log(2, 'INF|BkmsTelnet::read_until_prompt [%s]' % ret)
            if self._prompt in ret and ret[-1] == self._prompt:
                ret = ret[:ret.rfind(self._newline)]
                ret = ret.replace(command, '').lstrip()
                return ret

        raise AssertionError('read_until_prompt timeout')

    def read_until_newline(self, timeout=None):

        if not timeout:
            timeout = self._timeout

        ret = self.read_until(self._newline, timeout)
        self.L.log(2, 'INF| BkmsTelnet::read_until_newline resp [%s]'% ret)

        if not ret.endswith(self._newline):
            self.L.log(2, 
                'ERR| BkmsTelnet::read_until_newline timeout [%s]'% ret)
            raise AssertionError('read_until_newline timeout')

        return ret

    def _set_windows_size(self, rows, cols):
        if not self.can_naws:
            return

        self.windows_size = rows, cols
        size = struct.pack('!HH', cols, rows)
        self.sock.send(telnetlib.IAC + telnetlib.SB + telnetlib.NAWS + size + telnetlib.IAC + telnetlib.SE)

    def _negotiate_echo_on(self, sock, cmd, opt):
        # This is supposed to turn server side echoing on and turn other options off.
        if opt == telnetlib.ECHO and cmd in (telnetlib.WILL, telnetlib.WONT):
            self.sock.sendall(telnetlib.IAC + telnetlib.DO + opt)
        elif opt == telnetlib.NAWS and cmd in telnetlib.DO:
            self.sock.send(telnetlib.IAC + telnetlib.WILL + opt)
            self.can_naws = True
            self._set_windows_size(128, 128)
        elif opt != telnetlib.NOOPT:
            if cmd in (telnetlib.DO, telnetlib.DONT):
                self.sock.sendall(telnetlib.IAC + telnetlib.WONT + opt)
            elif cmd in (telnetlib.WILL, telnetlib.WONT):
                self.sock.sendall(telnetlib.IAC + telnetlib.DONT + opt)

    def is_dir(self, dirname):
        ret = ''
        try:
            ret = self.execute_command('find %s -type d' % dirname, 10)
        except:
            return False

        if not ret or 'find' in ret:
            return False
        return True

    def make_remote_dir(self, dirname):
        ret = ''
        try:
            ret = self.execute_command('mkdir %s' % dirname, 5)
        except:
            raise BkmsError.err_bkms_telnet('CMD_R_MAKE_DIR')

        if 'such' in ret:
            raise BkmsError.err_bkms_telnet('CMD_R_MAKE_DIR')

    def run_agent(self, ip, port, user, pwd):

        try:
            '''
            if '$' in user:
                user = user.replace('$', '\$')
            if '$' in pwd:
                pwd  = pwd.replace('$', '\$')
            if '!' in pwd:
                pwd  = pwd.replace('!', '\!')
            '''

            self.L.log(1, 'INF| BkmsTelnet::run_agent %s %s %s %s' % \
                     (ip, port, user, pwd))
          
            ret = self.execute_command_until("~/TBD/bkms_ftpserver '%s' '%s' '%s' '%s'" % \
                    (ip, port, user, pwd), 'RUN_SUCCESS', 5)
        except:
            raise BkmsError.err_agent_running('CMD_TBD_UTIL_FAIL')

        if not 'RUN_SUCCESS' in ret:
            raise BkmsError.err_agent_running('CMD_TBD_UTIL_FAIL')

    def clear(self):
        try:
            self.close_connection()
        except: pass

