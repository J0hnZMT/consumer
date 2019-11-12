import pika
from configparser import ConfigParser


# access the config file
def config_open(filename, section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {} not found in the {} file'.format(section, filename))

    return db


config_file = 'setup.ini'
section = 'rabbit'
rabbit = config_open(config_file, section)
host_name = rabbit.get('host')
queue_name = rabbit.get('queue')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=host_name))
channel = connection.channel()

channel.queue_declare(queue=queue_name, durable=True)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
