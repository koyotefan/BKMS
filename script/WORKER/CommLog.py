import time
import os
import threading

class Log(object):
    def __init__(self, level):
        self.lock = threading.Lock()
        self.level = level

        self.t_dir = os.path.join(os.getenv('HOME'), 'log')
        self.file  = 'worker.log'

    #def init(self, dir, file):
    #    self.t_dir = dir
    #    self.file  = file

    def init(self, file):
        self.file  = file

    def log(self, level, contents):

        if level > self.level:
            return

        try:
            self.lock.acquire()
            file = open(self.__get_file_name__(), "a+")
            file.write(time.strftime('%m-%d %H:%M:%S') + ' %s\n' % contents)
            file.close()
        except :
            pass
        finally:
            self.lock.release()

    def __get_file_name__(self):

        t_dir = os.path.join(self.t_dir, time.strftime('%Y%m%d'))

        if not os.path.exists(t_dir):
            os.mkdir(t_dir)
        elif os.path.isfile(t_dir):
            os.unlink(t_dir)
            os.mkdir(t_dir)
        else:
            pass

        return os.path.join(t_dir, self.file)

