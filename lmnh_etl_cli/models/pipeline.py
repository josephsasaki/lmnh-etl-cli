'''Module containing the pipeline class.'''

from os import environ
import logging
from logging import Logger
from argparse import Namespace
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection
from dotenv import load_dotenv
from confluent_kafka import Consumer

from models.extractor import Extractor
from models.transformer import Transformer
from models.loader import Loader

# Other
LOG_FILE_NAME = "invalid_messages.log"
ENV_FILE_PATH = '../.env'


class Pipeline():
    '''Class representing the entire pipeline process for extracting, 
    transforming and loading the kiosk event data.'''
    # pylint: disable=too-few-public-methods

    @staticmethod
    def _create_consumer() -> Consumer:
        '''Create a kafka consumer based on configuration in the .env file.'''
        kafka_config = {
            'bootstrap.servers': environ['BOOTSTRAP_SERVERS'],
            'security.protocol': environ['SECURITY_PROTOCOL'],
            'sasl.mechanisms': environ['SASL_MECHANISM'],
            'sasl.username': environ['USERNAME'],
            'sasl.password': environ['PASSWORD'],
            'group.id': environ['GROUP_ID'],
            'auto.offset.reset': environ['AUTO_OFFSET_RESET']
        }
        consumer = Consumer(kafka_config)
        consumer.subscribe([environ['TOPIC']])
        return consumer

    @staticmethod
    def _get_database_connection() -> connection:
        '''Get connection to the AWS RDS database.'''
        return psycopg2.connect(
            user=environ["DATABASE_USERNAME"],
            host=environ["DATABASE_IP"],
            database=environ["DATABASE_NAME"],
            port=environ["DATABASE_PORT"],
            password=environ["DATABASE_PASSWORD"]
        )

    @staticmethod
    def _create_custom_logger() -> Logger:
        '''Define the logger which will be used to record invalid messages'''
        logger = logging.getLogger(__name__)
        logger.addHandler(
            logging.FileHandler(LOG_FILE_NAME,
                                mode="a", encoding="utf-8")
        )
        return logger

    def __init__(self, args: Namespace) -> 'Pipeline':
        '''Initialise the pipeline with settings passed from the command line as arguments.'''
        load_dotenv(ENV_FILE_PATH)
        self.__consumer = Pipeline._create_consumer()
        self.__rds_conn = Pipeline._get_database_connection()
        logger = Pipeline._create_custom_logger() if args.l else None
        # ETL objects
        self.__extractor = Extractor(self.__consumer)
        self.__transformer = Transformer(self.__rds_conn, logger)
        self.__loader = Loader(self.__rds_conn)

    def run(self) -> None:
        '''Run the pipeline.'''
        try:
            while True:
                # Extraction
                message = self.__extractor.pull_message()
                if message is None:
                    continue
                # Transformation
                kiosk_event = self.__transformer.create_kiosk_event(message)
                if kiosk_event is None:
                    continue
                # Loading
                self.__loader.load_kiosk_event(kiosk_event)
        except KeyboardInterrupt:
            pass
        finally:
            self.__consumer.close()
            self.__rds_conn.close()
