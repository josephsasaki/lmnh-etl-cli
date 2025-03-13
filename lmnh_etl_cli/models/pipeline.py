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

from lmnh_etl_cli.models.extractor import Extractor
from lmnh_etl_cli.models.transformer import Transformer
from lmnh_etl_cli.models.loader import Loader


LOG_FILE_NAME = "invalid_messages.log"
ENV_FILE_PATH = './.env'
INITIALISE_MESSAGE = "PIPELINE SUCCESSFUL INITIALISED"
ERROR_MESSAGE = "PIPELINE INITIALISATION ERROR: %s"
BEGIN_RUN_MESSAGE = "RUNNING..."
FINISH_RUN_MESSAGE = "\nPIPELINE CLOSED"


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
            'auto.offset.reset': environ['AUTO_OFFSET_RESET'],
            "log_level": 4
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
        self.__consumer = None
        self.__rds_conn = None
        try:
            self.__consumer = Pipeline._create_consumer()
            self.__rds_conn = Pipeline._get_database_connection()
            logger = Pipeline._create_custom_logger() if args.l else None
            # ETL objects
            self.__extractor = Extractor(self.__consumer)
            self.__transformer = Transformer(self.__rds_conn, logger)
            self.__loader = Loader(self.__rds_conn)
            print(INITIALISE_MESSAGE)
        except Exception as e:
            print(ERROR_MESSAGE.format(e))
            self._cleanup()
            raise  # Re-raise the exception after cleanup

    def _cleanup(self):
        '''Ensure proper cleanup of open connections.'''
        if self.__consumer:
            self.__consumer.close()
            self.__consumer = None
        if self.__rds_conn:
            self.__rds_conn.close()
            self.__rds_conn = None

    def run(self) -> None:
        '''Run the pipeline.'''
        print(BEGIN_RUN_MESSAGE)
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
            print(FINISH_RUN_MESSAGE)
            self._cleanup()
