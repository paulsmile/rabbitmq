from pika import BlockingConnection, ConnectionParameters, PlainCredentials, BasicProperties
import json
import random


conn = BlockingConnection(
    ConnectionParameters(
        host='127.0.0.1',
        port=5672,
        virtual_host='/',
        credentials=PlainCredentials('guest', 'guest')
    )
)
channel = conn.channel()
channel.queue_declare(queue='queue1', durable=True)

for i in range(100):
    data = {
        'msg': str(random.randint(0, 10000)) +  'produces'
    }
    channel.basic_publish(
        exchange='test',
        routing_key='key1',
        body=json.dumps(data),
        properties=BasicProperties(delivery_mode=2)
    )
print 'msg ok!'