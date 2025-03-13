# pylint: skip-file

import pytest
from unittest.mock import Mock, patch

from lmnh_etl_cli.models.extractor import Extractor

MESSAGE_VALUE = '{"at": "2025-03-10T15:45:54.600066+00:00","site": "4", "val": -1, "type": 0}'


@pytest.fixture
def test_extractor_with_mock_consumer():
    '''Fixture which produces an extractor object used for testing.'''
    mock_consumer = Mock()
    test_extractor = Extractor(consumer=mock_consumer)
    return test_extractor, mock_consumer


def test_pull_message_none(test_extractor_with_mock_consumer):
    test_extractor, mock_consumer = test_extractor_with_mock_consumer
    mock_consumer.poll.return_value = None
    assert test_extractor.pull_message() is None


def test_pull_message_error(test_extractor_with_mock_consumer):
    test_extractor, mock_consumer = test_extractor_with_mock_consumer
    mock_message = Mock()
    mock_message.error.return_value = "ERROR"
    mock_consumer.poll.return_value = mock_message
    assert test_extractor.pull_message() is None


def test_pull_message_valid(test_extractor_with_mock_consumer):
    test_extractor, mock_consumer = test_extractor_with_mock_consumer
    mock_message = Mock()
    mock_message.error.return_value = None
    mock_message.value.return_value = MESSAGE_VALUE.encode('utf-8')
    mock_consumer.poll.return_value = mock_message
    assert test_extractor.pull_message() == MESSAGE_VALUE
