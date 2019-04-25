import pika
import requests
import json

class publisher:
    def __init__(self, host='localhost', port=5672, user='guest', passwd='guest'):
        self.HOST = host
        self.TCP_PORT = port
        self.USER = user
        self.PASSWD = passwd
        self.EXCHANGE = 'questions'
        self.MANAGEMENT_PORT = 15672

        credentials = pika.PlainCredentials(self.USER, self.PASSWD)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.HOST, self.TCP_PORT,
                '/', credentials))
        self.channel = self.connection.channel()

    def __serialize_msg(self, question, subscribers):
        dict = {}
        dict['question'] = question
        dict['subscribers'] = subscribers
        data = json.dumps(dict)
        return data

    # returns a list of channels
    def get_channels(self):
        url = 'http://%s:%s/api/queues' % (self.HOST, self.MANAGEMENT_PORT)
        r = requests.get(url, auth=(self.USER, self.PASSWD))
        res = []
        for json in r.json():
            res.append(json["name"])
        return res

    # returns a list of subscribers
    def get_subscribers(self):
        url = 'http://%s:%s/api/consumers' % (self.HOST, self.MANAGEMENT_PORT)
        r = requests.get(url, auth=(self.USER, self.PASSWD))
        res = []
        for json in r.json():
            res.append(json["consumer_tag"])
        return res

    def publish_question(self, channel_name, question, subscribers = "any"):
        data = self.__serialize_msg(question, subscribers)
        self.channel.basic_publish(exchange=self.EXCHANGE,
                      routing_key=channel_name,
                      body=data,
                      properties=pika.BasicProperties(delivery_mode = 2))

def parse_input(choice):
    pub = publisher()
    if choice == 1:
        print(pub.get_channels())
    elif choice == 2:
        print(pub.get_subscribers())
    elif choice == 3:
        channel_name = 'machine-learning'
        question = 'What is machine learning?'
        subscribers = ['subscriber_1']
        pub.publish_question(channel_name, question, subscribers)

if __name__ == "__main__":
    print("1. Get channels")
    print("2. Get subscribers")
    print("3. Publish question")
    choice = int(input("Enter your choice: "))
    parse_input(choice)

