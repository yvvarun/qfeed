import pika
import requests
import json

from modules.config_parser import ConfigParser

class Subscriber:
    def __init__(self):
        self.__cfg_dict = dict()
        self.__cfg_file_path = 'configs/publisher.ini'
        self.__parser = ConfigParser()
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
        self.__cfg_dict['management_port'] = self.__parser.get_management_port()

    def connect(self):
        """
        connects to a rabbitmq broker
        """

        credentials = pika.PlainCredentials(self.__cfg_dict['user'],
                                            self.__cfg_dict['password'])
        conn_params = pika.ConnectionParameters(self.__cfg_dict['host'],
                                                self.__cfg_dict['tcp_port'],
                                                '/', credentials)
        self.__connection = pika.BlockingConnection(conn_params)
        self.__channel = self.__connection.channel()

    def get_channels(self) -> list:
        """
        returns a list of channels available
        to which the user can subscribe to
        """

        url = 'http://%s:%s/api/queues' % (self.__cfg_dict['host'],
                self.__cfg_dict['management_port'])
        r = requests.get(url, auth=(self.__cfg_dict['user'],
            self.__cfg_dict['password']))
        res = []
        for json in r.json():
            res.append(json["name"])
        return res

    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        delivery_tag = method.delivery_tag
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def subscribe(self, email, exchange, channel_list):
        for chn in channel_list:
            result = self.__channel.queue_declare(queue=chn, durable=True)
            queue_name = result.method.queue
            self.__channel.queue_bind(exchange=exchange, queue=queue_name)
            print("queue_name is {queue_name}".format(queue_name=queue_name))
            self.__channel.basic_consume(queue=queue_name,
                          consumer_tag=email,
                          on_message_callback=self.callback)

    def consume(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.__channel.start_consuming()

def test():
    """
    test config parser
    """
    p = ConfigParser()
    config_file_name = "configs/subscriber.ini"
    if p.parse(config_file_name):
        print(p.get_user())
        print(p.get_password())

    """
    test Subscriber API
    """
    sub = Subscriber()
    sub.connect()
    channels = sub.get_channels()
    print(channels)
    if len(channels) > 0:
        channel_list = []
        channel_list.append(channels[0])
        email = "yvvarun@amagi.com"
        exchange = 'questions'
        sub.subscribe(email, exchange, channel_list)
        sub.consume()

if __name__ == "__main__":
    test()


