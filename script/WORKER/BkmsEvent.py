
class BkmsEvent(object):
    def __init__(self, iLog):
        self.L = iLog

        self.resource_list = [0,0,0,0]
        self.in_service    = True
        self.is_success    = True
        self.err_reason    = ''

        self.grade         = 0

        self.limit = {}

        self.limit['CPU'] = [] 
        self.limit['MEM'] = [] 
        self.limit['DISK']= []
        self.limit['NET'] = [] 

        self.value = {}

        self.value['CPU']  = 0
        self.value['MEM']  = 0
        self.value['DISK'] = 0
        self.value['NET']  = 0

        self.tag = {0:'CPU',1:'MEM',2:'DISK',3:'NET'}

    def set_cpu_limit(self, minor, major, critical):
        self.set_limit(self.tag[0], minor, major, critical)

    def set_mem_limit(self, minor, major, critical):
        self.set_limit(self.tag[1], minor, major, critical)

    def set_disk_limit(self, minor, major, critical):
        self.set_limit(self.tag[2], minor, major, critical)

    def set_net_limit(self, minor, major, critical):
        self.set_limit(self.tag[3], minor, major, critical)

    def set_limit(self, tag, minor, major, critical):
        self.limit[tag].append(minor)
        self.limit[tag].append(major)
        self.limit[tag].append(critical)

    def is_event(self):
        return self.grade > 0

    def sleep_time(self):
        return self.grade * 3

    def gather_cpu_val(self, val):
        self.__update_val__(0, val)

    def gather_mem_val(self, val):
        self.__update_val__(1, val)

    def gather_disk_val(self, val):
        self.__update_val__(2, val)

    def gather_network_val(self, val):
        self.__update_val__(3, val)

    def is_service(self):
        return self.in_service

    def set_terminate(self, result, err_reason):
        self.is_success = result
        self.err_reason = err_reason
        self.in_service = False
        self.L.log(1, 'INF| BkmsEvent::set_terminate serv:%s result:%s err:%s' % \
                (self.in_service, self.is_success, self.err_reason))

    def get_terminate(self):
        return (self.in_service, self.is_success, self.err_reason)

    def __update_val__(self, index, val):
    
        self.resource_list[index] = 0

        for i in range(3):
            if int(val) >= int(self.limit[self.tag[index]][i]):
                self.L.log(0, 'BkmsEvent::update_val r:%s val:%s limit:%s' % \
                   ((self.tag[index], val, self.limit[self.tag[index]][i])))
                self.resource_list[index] += 1

        max = 0

        for i in range(4):
            if self.resource_list[i] > max:
                max = self.resource_list[i]

        self.grade = max

        if int(val) > int('100'):
            val = '100'

        self.value[self.tag[index]] = val

    def get_value(self, tag):
        ret = ''
        try:
            ret = self.value[tag]
        except:
            ret = '0'
        return ret
