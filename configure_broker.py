import pika

class broker:
    def __init__(self, host='localhost', port=5672, user='guest', passwd='guest'):
        self.HOST = host
        self.TCP_PORT = port
        self.USER = user
        self.PASSWD = passwd
        self.EXCHANGE = 'questions'
        self.QUEUES = ['machine-learning']
        self.MANAGEMENT_PORT = 15672

        credentials = pika.PlainCredentials(self.USER, self.PASSWD)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.HOST, self.TCP_PORT,
                '/', credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE,
                exchange_type='direct', durable=True)

        for q in self.QUEUES:
            self.channel.queue_declare(queue=q, durable=True)

if __name__ == "__main__":
    broker = broker()
