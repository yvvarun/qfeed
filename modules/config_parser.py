import sys

class ConfigParser:
    def __init__(self):
        # private properties.
        self.__configfile = ''
        self.__config = {}

    def parse(self, configfile):
        self.__configfile = configfile
        try:
            with open(self.__configfile) as fd:
                for line in fd:
                    line = line.strip()

                    # ignore blank lines
                    if not line:
                        continue

                    # ignore comments
                    if line[0] == '#':
                        continue

                    name, val = line.partition("=")[::2]
                    name = name.strip()
                    val = val.strip()

                    if name:
                        self.__config[name] = val
        except Exception as e:
            print(e)
            return False

        return True

    def get_user(self):
        if "user" in self.__config:
            return (str(self.__config['user']))
        else:
            return ""

    def get_password(self):
        if "password" in self.__config:
            return (str(self.__config['password']))
        else:
            return ""

    def get_host(self):
        if "host" in self.__config:
            return (str(self.__config['host']))
        else:
            return ""

    def get_tcp_port(self):
        if "tcp_port" in self.__config:
            return (int(self.__config['tcp_port']))
        else:
            return -1

    def get_management_port(self):
        if "management_port" in self.__config:
            return (int(self.__config['management_port']))
        else:
            return -1

    def get_exchange(self):
        if "exchange" in self.__config:
            return (str(self.__config['exchange']))
        else:
            return ""

    def get_channels(self):
        if "channels" in self.__config:
            return (str(self.__config['channels']))
        else:
            return ""

    def get_email(self):
        if "email" in self.__config:
            return (str(self.__config['email']))
        else:
            return ""


###############################################################################
# TEST SECTION
###############################################################################

def test():
    p = ConfigParser()

    config_file_name = "../configs/broker.ini"
    if p.parse(config_file_name):
        print(p.get_tcp_port())
        print(p.get_channels())
    else:
        print("parse failed for %s" % (config_file_name))

if __name__ == '__main__':
    test()
