
class Error(Exception): pass

class err_login(Error): pass
class err_nocommand(Error): pass
class err_not_implements(Error): pass
class err_timeout(Error): pass
class err_not_allow(Error): pass
class err_send(Error): pass
class err_recv(Error): pass
class err_agent_running(Error): pass
class err_work_same_size(Error): pass
class err_download_write(Error): pass
class err_upload_read(Error): pass
class err_service_term(Error): pass

class err_bkms_telnet(Error): pass
class err_remote_system(Error): pass
class err_sun_system(Error): pass
class err_hpux_system(Error): pass
class err_linux_system(Error): pass
class err_bkms_client(Error): pass
class err_bkms_ftp(Error): pass
class err_bkms_work(Error): pass
class err_bkms_dir(Error): pass
class err_bkms_message(Error): pass
