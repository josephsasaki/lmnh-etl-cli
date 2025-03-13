'''Module containing kiosk event class.'''

from datetime import datetime


class KioskEvent():
    '''Class representing a single kiosk event, involving either a rating or request.'''

    def __init__(self, event_at: datetime, exhibition_id: int, is_rating: bool,
                 rating_or_request_id: int) -> 'KioskEvent':
        '''Instantiate a kiosk event object'''
        self.__event_at = event_at
        self.__exhibition_id = exhibition_id
        self.__is_rating = is_rating
        self.__rating_or_request_id = rating_or_request_id

    def get_insert_param(self) -> tuple[int, int, datetime]:
        '''Return the data as a tuple, ready to be loaded.'''
        return self.__exhibition_id, self.__rating_or_request_id, self.__event_at

    def is_rating(self) -> bool:
        '''Get whether the event was for a rating or request.'''
        return self.__is_rating
