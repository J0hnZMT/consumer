import pika
from configparser import ConfigParser
import yaml
import logging.config
import logging
import os


# setup the logger
def setup_logging(default_path, default_level, env_key):
    """ Setup logging configuration """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level, filename='logs.log',
                            format="%(asctime)s:%(name)s:%(levelname)s:%(message)s")
        print('Failed to load configuration file. Using default configs')


""" start the logging function """
path = "logging.yaml"
level = logging.INFO
env = 'LOG_CFG'
setup_logging(path, level, env)
logger = logging.getLogger(__name__)


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
    logger.info(" [x] Received %r" % body)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

logger.info(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
