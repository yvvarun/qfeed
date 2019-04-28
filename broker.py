import pika

from modules.config_parser import ConfigParser

class Broker:
    def __init__(self):
        self.__cfg_dict      = dict()
        self.__cfg_file_path = 'configs/broker.ini'
        self.__parser        = ConfigParser()
        self.__parse_config()

    def __parse_config(self):
        """
        parses config file and stores configs in a dict
        """

        self.__cfg_dict = dict()
        self.__parser = ConfigParser()
        ret = self.__parser.parse(self.__cfg_file_path)
        if ret == False:
            print('Failed to parse {cfg_file}, exiting'
                    .format(cfg_file=self.__cfg_file_path))
            sys.exit(-1)

        self.__cfg_dict['user']            = self.__parser.get_user()
        self.__cfg_dict['password']        = self.__parser.get_password()
        self.__cfg_dict['host']            = self.__parser.get_host()
        self.__cfg_dict['tcp_port']        = self.__parser.get_tcp_port()
        self.__cfg_dict['exchange']        = self.__parser.get_exchange()
        self.__cfg_dict['channels']        = self.__parser.get_channels().split(',')

    def configure(self) -> bool:
        """
        configures broker with exchange and queues
        """

        try:
            credentials = pika.PlainCredentials(self.__cfg_dict['user'],
                                                self.__cfg_dict['password'])
            conn_params = pika.ConnectionParameters(self.__cfg_dict['host'],
                                               self.__cfg_dict['tcp_port'],
                                               '/', credentials)
            self.__connection = pika.BlockingConnection(conn_params)
            self.__channel = self.__connection.channel()
            self.__channel.exchange_declare(exchange=self.__cfg_dict['exchange'],
                                        exchange_type='direct', durable=True)

            for chn in self.__cfg_dict['channels']:
                self.__channel.queue_declare(queue=chn, durable=True)
        except Exception as e:
            print(e)
            return False

        return True

    def clear_configurations(self) -> bool:
        """
        clears the existing broker configurations
        """

        try:
            self.__channel.exchange_delete(self.__cfg_dict['exchange'])
        except ValueError:
            print('Invalid exchange.')
            return False

        try:
            for chn in self.__cfg_dict['channels']:
                self.__channel.queue_delete(chn)
        except ValueError:
            print('Invalid channels.')
            return False

        return True

###############################################################################
# TEST SECTION
###############################################################################

def test():
    """
    test config parser
    """
    p = ConfigParser()
    config_file_name = "configs/broker.ini"
    if p.parse(config_file_name):
        print(p.get_tcp_port())
        print(p.get_channels())
    else:
        print("parse failed for %s" % (config_file_name))

    """
    test Broker API
    """
    broker = Broker()
    broker.configure()
    #broker.clear_configurations()

if __name__ == '__main__':
    test()
