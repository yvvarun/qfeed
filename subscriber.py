import pika
import requests
import json

class Subscriber:
    def __init__(self, host='localhost', port=5672, user='guest', passwd='guest'):
        self.HOST = host
        self.TCP_PORT = port
        self.USER = user
        self.PASSWD = passwd
        self.MANAGEMENT_PORT = 15672

        credentials = pika.PlainCredentials(self.USER, self.PASSWD)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.HOST, self.TCP_PORT,
                '/', credentials))
        self.channel = self.connection.channel()

    # returns a list of channels
    def get_channels(self):
        url = 'http://%s:%s/api/queues' % (self.HOST, self.MANAGEMENT_PORT)
        r = requests.get(url, auth=(self.USER, self.PASSWD))
        res = []
        for json in r.json():
            res.append(json["name"])
        return res

    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        delivery_tag = method.delivery_tag
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def subscribe(self, channel_list):
        for chn in channel_list:
            result = self.channel.queue_declare(queue=chn, durable=True)
            queue_name = result.method.queue
            self.channel.queue_bind(exchange='questions', queue=queue_name)
            self.channel.basic_consume(queue=queue_name,
                          consumer_tag="subsciber_1",
                          on_message_callback=self.callback)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

def parse_input(choice):
    sub = Subscriber()
    if choice == 1:
        print(sub.get_channels())
    elif choice == 2:
        channel_names = input("Enter channels to subscribe to (comma separated): ")
        channel_list = channel_names.split(',')
        sub.subscribe(channel_list)
        sub.start_consuming()

if __name__ == "__main__":
    print("1. Get channels")
    print("2. Subscribe to channels")
    choice = int(input("Enter your choice: "))
    parse_input(choice)


