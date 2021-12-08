
import time
import sys 
import traceback

import BkmsTelnet
import RemoteSystem

class BkmsRemoteResource(object):
    def __init__(self, iLog, event):
        self.L       = iLog
        self.event   = event 

    def init(self, ip, port, user, pwd, prompt):
        self.ip      = ip
        self.port    = port
        self.user    = user
        self.pwd     = pwd
        self.prompt  = prompt

        return True

    def cpu(self):

        self.L.log(0, 'INF| BkmsRemoteResource::thread start')

        itelnet = None

        try:
            itelnet = BkmsTelnet.BkmsTelnet(self.L, self.ip, self.port)
            itelnet.login(self.user, self.pwd)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| BkmsRemoteResource::cpu login fail :%s' % \
            repr(traceback.format_exception( exc_type, exc_value, 
                                             exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::cpu exit")
            itelnet.clear()

            return

        self.L.log(0, 'INF| BkmsRemoteResource::cpu login success')

        try:
            system = RemoteSystem.System().create_instance(itelnet, self.L)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
              "ERR| BkmsRemoteResource::cpu create system instance fail :%s" \
               % repr(traceback.format_exception( exc_value, exc_value, 
                                                  exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::cpu thread exit")
            itelnet.clear()

            return

        self.L.log(0, 'INF| BkmsRemoteResource::cpu create system')

        while self.event.is_service():
            try:
                val = system.get_cpu(itelnet)
                self.event.gather_cpu_val(val)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsRemoteResource::cpu except :%s' %  \
                repr(traceback.format_exception(exc_type, exc_value,
                                                exc_traceback)))
                break

        self.L.log(0, 'INF| BkmsRemoteResource::cpu thread terminate')
        itelnet.clear()
        return

    def memory(self):

        self.L.log(0, 'INF| BkmsRemoteResource::memory thread start')

        itelnet = None

        try:
            itelnet = BkmsTelnet.BkmsTelnet(self.L, self.ip, self.port)
            itelnet.login(self.user, self.pwd) 
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| BkmsRemoteResource::memory login fail :%s' % \
            repr(traceback.format_exception( exc_type, exc_value, 
                                             exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::memory exit")
            itelnet.clear()

            return
        self.L.log(0, 'INF| BkmsRemoteResource::memory login success')

        try:
            system = RemoteSystem.System().create_instance(itelnet, self.L)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
                "ERR| BkmsRemoteResource::memory create system fail:%s" \
                 % repr(traceback.format_exception( exc_value, exc_value,  
                                                    exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::memory thread exit")
            itelnet.clear()

            return

        self.L.log(0, 'INF| BkmsRemoteResource::memory create system')

        while self.event.is_service():
            try:
                val = system.get_mem(itelnet)
                self.L.log(0, '##### BkmsRemoteResource::mem val:%s' % val) 
                self.event.gather_mem_val(val)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsRemoteResource::memory except :%s' %  \
                repr(traceback.format_exception(exc_type, exc_value,
                                                exc_traceback)))
                break

        self.L.log(0, 'INF| BkmsRemoteResource::memory thread terminate')
        itelnet.clear()
        return

    def disk(self):
        self.L.log(0, 'INF| BkmsRemoteResource::disk thread start')

        itelnet = None 

        try:
            itelnet = BkmsTelnet.BkmsTelnet(self.L, self.ip, self.port)
            itelnet.login(self.user, self.pwd) 
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| BkmsRemoteResource::disk login fail :%s' % \
            repr(traceback.format_exception( exc_type, exc_value, 
                                             exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::disk exit")
            itelnet.clear()

            return
        self.L.log(0, 'INF| BkmsRemoteResource::disk login success')

        try:
            system = RemoteSystem.System().create_instance(itelnet, self.L)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
                "ERR| BkmsRemoteResource::disk create system fail:%s" \
                 % repr(traceback.format_exception( exc_value, exc_value,  
                                                    exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::disk thread exit")
            itelnet.clear()

            return

        self.L.log(0, 'INF| BkmsRemoteResource::disk create system')

        while self.event.is_service():
            try:
                val = system.get_disk(itelnet)
                self.event.gather_disk_val(val)
                time.sleep(1)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsRemoteResource::disk except :%s' %  \
                repr(traceback.format_exception(exc_type, exc_value,
                                                exc_traceback)))
                break

        self.L.log(0, 'INF| BkmsRemoteResource::disk thread terminate')
        itelnet.clear()
        return

    def network(self):
        self.L.log(0, 'INF| BkmsRemoteResource::net thread start')

        itelnet = None

        try:
            itelnet = BkmsTelnet.BkmsTelnet(self.L, self.ip, self.port)
            itelnet.login(self.user, self.pwd) 
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 'ERR| BkmsRemoteResource::net login fail :%s' % \
            repr(traceback.format_exception( exc_type, exc_value, 
                                             exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::net exit")
            itelnet.clear()

            return

        self.L.log(0, 'INF| BkmsRemoteResource::network login success')

        try:
            system = RemoteSystem.System().create_instance(itelnet, self.L)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0, 
                "ERR| BkmsRemoteResource::net create system fail:%s" \
                 % repr(traceback.format_exception( exc_value, exc_value,  
                                                    exc_traceback)))
            self.L.log(0, "ERR| BkmsRemoteResource::net thread exit")
            itelnet.clear()

            return

        self.L.log(0, 'INF| BkmsRemoteResource::network create system')

        while self.event.is_service():
            try:
                val = system.get_network(itelnet)
                self.event.gather_network_val(val)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0, 'ERR| BkmsRemoteResource::net except :%s' %  \
                repr(traceback.format_exception(exc_type, exc_value,
                                                exc_traceback)))
                break

        self.L.log(0, 'INF| BkmsRemoteResource::net thread terminate')
        itelnet.clear()
        return
