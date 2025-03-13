'''Module containing the transformer class.'''

from logging import Logger
from datetime import datetime
import json
from psycopg2.extensions import connection

from lmnh_etl_cli.models.kiosk_event import KioskEvent


EXHIBITION_QUERY = 'SELECT public_id, exhibition_id FROM exhibition;'
RATING_QUERY = 'SELECT rating_value, rating_id FROM rating;'
REQUEST_QUERY = 'SELECT request_value, request_id FROM request;'
EXHIBITION_PREFIX = 'EXH_'
START_TIME = "08:45:00"
END_TIME = "18:15:00"
RATING_VALUE_WHEN_REQUEST = -1


class Transformer():
    '''Class representing the transformation part of the pipeline. This object takes
    the extracted message data and cleans it to ensure it is in a valid form for uploading.
    A part of the transformation process is identifying the exhibition and rating/request ids.
    '''
    # pylint: disable=too-few-public-methods

    @staticmethod
    def _create_id_maps(rds_conn: connection) -> list[dict]:
        '''Connect to the database and retrieve reference tables. 
        Then, produce dicts which map values to primary keys.'''
        curs = rds_conn.cursor()
        # Exhibitions
        # -> [('EXH_01', 1), ('EXH_02', 2), ...]
        curs.execute(EXHIBITION_QUERY)
        exhibitions_map = dict(curs.fetchall())  # ->
        # Ratings
        curs.execute(RATING_QUERY)
        ratings_map = dict(curs.fetchall())
        # Requests
        curs.execute(REQUEST_QUERY)
        requests_map = dict(curs.fetchall())
        # Close and return
        curs.close()
        return exhibitions_map, ratings_map, requests_map

    def __init__(self, rds_conn: connection, logger: Logger | None):
        '''Instantiate the transformer, using the connection to get the exhibition, 
        request and ratings data.'''
        self.__logger = logger
        exhibitions_map, ratings_map, requests_map = Transformer._create_id_maps(
            rds_conn)
        self.__exhibition_map = exhibitions_map
        self.__rating_map = ratings_map
        self.__request_map = requests_map

    def _get_json(self, message_value: str) -> tuple[bool, str]:
        try:
            return True, json.loads(message_value)
        except ValueError:
            return False, 'Kafka message was not in json format.'

    def _has_valid_keys(self, message_data: dict) -> tuple[bool, str]:
        # Check the 'at' attribute is present
        if 'at' not in message_data:
            return False, 'Kiosk event missing "at" attribute.'
        # Check the 'site' attribute is present
        if 'site' not in message_data:
            return False, 'Kiosk event missing "site" attribute.'
        # Check the 'val' attribute is present
        if 'val' not in message_data:
            return False, 'Kiosk event missing "val" attribute.'
        # Check the 'type' attribute is present if the value is -1
        if message_data['val'] == -1 and 'type' not in message_data:
            return False, 'Kiosk event missing "type" attribute.'
        # Check no other keys are present
        if (message_data['val'] != -1 and len(message_data.keys()) != 3) or \
                (message_data['val'] == -1 and len(message_data.keys()) != 4):
            return False, 'Kiosk event contains unexpected attributes.'
        return True, None

    def _has_valid_values(self, message_data: dict) -> tuple[bool, str]:
        # Check the 'at' attribute is in a valid format
        try:
            kiosk_event_at = datetime.strptime(
                message_data['at'], "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError:
            return False, 'Kiosk event date-time is invalid.'
        # Check the kiosk event occurred at a valid time.
        kiosk_event_time = kiosk_event_at.time()
        start_time = datetime.strptime(START_TIME, "%H:%M:%S").time()
        end_time = datetime.strptime(END_TIME, "%H:%M:%S").time()
        if not start_time <= kiosk_event_time <= end_time:
            return False, 'Kiosk event occurred at invalid time.'
        # Check the site value is valid
        public_id = f"{EXHIBITION_PREFIX}{message_data['site'].zfill(2)}"
        if public_id not in self.__exhibition_map.keys():
            return False, 'Kiosk event has invalid exhibition id.'
        # Check the val attribute is valid
        if message_data['val'] not in list(self.__rating_map.keys()) + [RATING_VALUE_WHEN_REQUEST]:
            return False, 'Kiosk event has an invalid rating value.'
        # Check the type value is valid if present
        if 'type' in message_data:
            if message_data['type'] not in self.__request_map.keys():
                return False, 'Kiosk event has an invalid request value.'
        return True, None

    def _log_invalid_message(self, message_value: str, issue: str) -> None:
        '''Log an invalid pulled message to a log, along with the reason'''
        text = f'Invalid pulled message:\n\t{message_value}\nIssue: {issue}'
        self.__logger.error(text)

    def _transform(self, message_data: dict) -> KioskEvent:
        '''Using the valid pulled message, create a kiosk event.'''
        # Get and clean the message data
        event_at = message_data['at']
        exhibition_public_id = f"{EXHIBITION_PREFIX}{str(message_data['site']).zfill(2)}"
        rating_value = message_data['val']
        request_value = message_data.get('type')
        # Transform into appropriate values for loading
        event_datetime = datetime.strptime(event_at, "%Y-%m-%dT%H:%M:%S.%f%z")
        exhibition_id = self.__exhibition_map[exhibition_public_id]
        is_rating = request_value is None
        if is_rating:
            rating_or_request_id = self.__rating_map[rating_value]
        else:
            rating_or_request_id = self.__request_map[request_value]
        # Create and return kiosk event
        return KioskEvent(event_datetime, exhibition_id, is_rating, rating_or_request_id)

    def create_kiosk_event(self, message_value: str) -> KioskEvent | None:
        '''Given the value from a message, check the value is valid, clean it and then
        produce a kiosk event containing all the appropriate information.'''

        is_json, message_data = self._get_json(message_value)
        if not is_json:
            if self.__logger is not None:
                self._log_invalid_message(message_value, message_data)
            return None

        has_valid_keys, issue = self._has_valid_keys(message_data)
        if not has_valid_keys:
            if self.__logger is not None:
                self._log_invalid_message(message_value, issue)
            return None

        has_valid_values, issue = self._has_valid_values(message_data)
        if not has_valid_values:
            if self.__logger is not None:
                self._log_invalid_message(message_value, issue)
            return None

        return self._transform(message_data)
