import pika
from configparser import ConfigParser
import yaml
import logging.config
import logging as logs
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
                logs.basicConfig(level=default_level)
    else:
        logs.basicConfig(level=default_level, filename='logs.log',
                         format="%(asctime)s:%(name)s:%(levelname)s:%(message)s")
        print('Failed to load configuration file. Using default configs')


""" start the logging function """
path = "logging.yaml"
level = logs.INFO
env = 'LOG_CFG'
setup_logging(path, level, env)
log = logs.getLogger(__name__)


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
host_name = rabbit['host']
queue_name = rabbit['queue']
delay = rabbit['delay']
connection = pika.BlockingConnection(pika.ConnectionParameters(host=host_name))
channel = connection.channel()
channel.queue_declare(queue=queue_name, durable=True)
while True:
    result = channel.basic_get(queue=queue_name, auto_ack=True)
    if result[2] is not None:
        log.info("[x] Received: {}".format(result[2]))
    else:
        log.info("Channel Empty. [*] Waiting for messages. To exit press CTRL+C")
        connection.sleep(int(delay))
