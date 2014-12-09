from pika import BlockingConnection, ConnectionParameters, PlainCredentials, BasicProperties


conn = BlockingConnection(
    ConnectionParameters(
        host='127.0.0.1',
        port=5672,
        virtual_host='/',
        credentials=PlainCredentials('guest', 'guest')
    )
)

def on_message(channel, method_frame, header_frame, body):
    channel.queue_declare(queue=body, auto_delete=True)

    if body.startswith("queue:"):
        queue = body.replace("queue:", "")
        key = body + "_key"
        print("Declaring queue %s bound with key %s" %(queue, key))
        channel.queue_declare(queue=queue, auto_delete=True)
        channel.queue_bind(queue=queue, exchange="test_exchange", routing_key=key)
    else:
        print("Message body", body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

channel = conn.channel()
channel.exchange_declare(exchange="test", exchange_type="direct", passive=False, durable=True, auto_delete=False)
channel.queue_declare(queue='queue1', durable=True)
channel.queue_bind(queue="queue1", exchange="test", routing_key="key1")
channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_message, queue='queue1')
channel.start_consuming()
print 'consume ok'
conn.close()