'''Module containing the extractor class.'''

from confluent_kafka import Consumer


POLL_TIMEOUT_DURATION = 1.0


class Extractor():
    '''Class representing the extraction part of the pipeline. This object
    will connect to a kafka topic and pull messages, containing kiosk events.
    All kiosk events are then returned, including invalid ones.'''
    # pylint: disable=too-few-public-methods

    def __init__(self, consumer: Consumer):
        '''Initialise the extractor, in particular, the consumer pulling data.'''
        self.__consumer = consumer

    def pull_message(self) -> str:
        '''Pull messages from the kafka topic, convert to a string and return'''
        message = self.__consumer.poll(POLL_TIMEOUT_DURATION)
        # Check there is a message to extract
        if message is None:
            return None
        # Check the message isn't an error
        if message.error() is not None:
            return None
        # Create the contents of the message as a string
        return message.value().decode('utf-8')
