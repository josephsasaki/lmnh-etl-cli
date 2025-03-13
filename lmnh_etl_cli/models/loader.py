'''Module containing the loader class.'''

from psycopg2.extensions import connection

from lmnh_etl_cli.models.kiosk_event import KioskEvent


RATING_INSERT = '''
        INSERT INTO rating_interaction 
            (exhibition_id, rating_id, event_at)
        VALUES
            (%s, %s, %s);
    '''
REQUEST_INSERT = '''
        INSERT INTO request_interaction 
            (exhibition_id, request_id, event_at)
        VALUES
            (%s, %s, %s);
    '''


class Loader():
    '''Class representing the loading part of the pipeline. This class takes a cleaned kiosk
    event and adds it to the AWS RDS.'''
    # pylint: disable=too-few-public-methods

    def __init__(self, rds_conn: connection):
        '''Instantiate the loader with the connection to the AWS RDS.'''
        self.__rds_conn = rds_conn

    def load_kiosk_event(self, kiosk_event: KioskEvent):
        '''Take a kiosk event and load it to the database'''
        curs = self.__rds_conn.cursor()

        if kiosk_event.is_rating():
            curs.execute(RATING_INSERT, kiosk_event.get_insert_param())
        else:
            curs.execute(REQUEST_INSERT, kiosk_event.get_insert_param())

        self.__rds_conn.commit()
        curs.close()
