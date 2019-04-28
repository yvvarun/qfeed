import pika
import requests
import json

from modules.config_parser import ConfigParser

class Publisher:
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

    def __serialize_msg(self, question, subscribers):
        dict = {}
        dict['question'] = question
        dict['requested_subscribers'] = subscribers
        data = json.dumps(dict)
        return data

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
        to which the user can publish a question to
        """

        url = 'http://%s:%s/api/queues' % (self.__cfg_dict['host'],
                self.__cfg_dict['management_port'])
        r = requests.get(url, auth=(self.__cfg_dict['user'],
            self.__cfg_dict['password']))
        res = []
        for json in r.json():
            res.append(json["name"])
        return res

    def get_subscribers(self) -> list:
        """
        returns a list of subscribers available
        """

        url = 'http://%s:%s/api/consumers' % (self.__cfg_dict['host'],
                self.__cfg_dict['management_port'])
        r = requests.get(url, auth=(self.__cfg_dict['user'],
                                    self.__cfg_dict['password']))
        res = []
        for json in r.json():
            res.append(json["consumer_tag"])
        return res

    def publish_question(self, exchange, channel_name, question,
            subscribers = "any"):
        data = self.__serialize_msg(question, subscribers)
        print("routing key is {routing_key}".format(routing_key=channel_name))
        self.__channel.basic_publish(exchange=exchange,
                      routing_key=channel_name,
                      body=data,
                      properties=pika.BasicProperties(delivery_mode = 2))
        print("published")

def test():
    """
    test config parser
    """
    p = ConfigParser()
    config_file_name = "configs/publisher.ini"
    if p.parse(config_file_name):
        print(p.get_host())
        print(p.get_tcp_port())

    """
    test Publisher API
    """
    pub = Publisher()
    pub.connect()
    channels = pub.get_channels()
    print(channels)
    subscribers = pub.get_subscribers()
    print(subscribers)
    if len(channels) > 0 and len(subscribers) > 0:
        exchange = 'questions'
        channel_name = 'machine-learning'
        question = 'What is machine learning?'
        pub.publish_question(exchange, channels[0], question, subscribers[0])

if __name__ == "__main__":
    test()
