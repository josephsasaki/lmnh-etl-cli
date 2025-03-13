# pylint: skip-file

from unittest.mock import patch
import pytest
import logging
import json

from models.transformer import Transformer
from models.kiosk_event import KioskEvent


VALID_MESSAGE_VALUE = '{"at": "2025-03-10T15:45:54.600066+00:00","site": "1", "val": -1, "type": 0}'
INVALID_MESSAGE_VALUE = '{"at": "2025-03-10T15:45:54.600066+00:00","site": "2", "val": 4, "type": 0}'
RATING_MESSAGE_VALUE = '{"at": "2025-03-10T15:45:54.600066+00:00","site": "1", "val": 4}'
REQUEST_MESSAGE_VALUE = '{"at": "2025-03-10T15:45:54.600066+00:00","site": "2", "val": -1, "type": 1}'


@pytest.fixture
def mock_transformer():
    mock_id_maps = (
        {"EXH_01": 1, "EXH_02": 2, "EXH_03": 3},
        {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
        {0: 1, 1: 2},
    )
    with patch.object(Transformer, "_create_id_maps", return_value=mock_id_maps):
        transformer = Transformer(
            rds_conn=None, logger=logging.getLogger(__name__))
        yield transformer


@pytest.fixture
def mock_transformer_without_logger():
    mock_id_maps = (
        {"EXH_01": 1, "EXH_02": 2, "EXH_03": 3},
        {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
        {0: 1, 1: 2},
    )
    with patch.object(Transformer, "_create_id_maps", return_value=mock_id_maps):
        transformer = Transformer(
            rds_conn=None, logger=None)
        yield transformer


def test_get_json_valid(mock_transformer):
    valid, _ = mock_transformer._get_json(VALID_MESSAGE_VALUE)
    assert valid


def test_get_json_invalid(mock_transformer):
    valid, _ = mock_transformer._get_json("invalid json")
    assert not valid


def test_has_valid_keys_missing_at(mock_transformer):
    valid, _ = mock_transformer._has_valid_keys(
        {'site': '3', 'val': 2}
    )
    assert not valid


def test_has_valid_keys_missing_site(mock_transformer):
    valid, _ = mock_transformer._has_valid_keys(
        {'at': '2025-03-10T15:45:54.600066+00:00', 'val': -1, 'type': 'ERR'}
    )
    assert not valid


def test_has_valid_keys_missing_val(mock_transformer):
    valid, _ = mock_transformer._has_valid_keys(
        {'at': '2025-03-10T15:45:54.600066+00:00', 'site': '4', 'type': 'ERR'}
    )
    assert not valid


def test_has_valid_keys_missing_type(mock_transformer):
    valid, _ = mock_transformer._has_valid_keys(
        {'at': '2025-03-10T15:45:54.600066+00:00', 'site': '4', 'val': -1}
    )
    assert not valid


@pytest.mark.parametrize('message_data', [
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '4', 'val': 3, 'extra': 100},
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '4', 'val': -1, 'type': '0', 'extra': 100},
])
def test_has_valid_keys_extra_keys(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_keys(message_data)
    assert not valid


@pytest.mark.parametrize('message_data', [
    {"at": "2025-03-10T15:44:34.590745+00:00", "site": "5", "val": 4},
    {"at": "2025-03-10T15:44:34.590745+00:00", "site": "0", "val": -1, "type": 0},
    {"at": "2025-03-10T15:44:34.590745+00:00", "site": "0", "val": -1, "type": 1},
    {"at": "2025-03-10T09:44:34.590745+00:00", "site": "1", "val": 0},
    {"at": "2025-03-10T12:44:34.590745+00:00", "site": "2", "val": 1},
    {"at": "2025-03-10T14:44:34.590745+00:00", "site": "3", "val": 2},
    {"at": "2025-03-10T16:44:34.590745+00:00", "site": "4", "val": 3},
    {"at": "2025-03-10T17:44:34.590745+00:00", "site": "5", "val": 4},
])
def test_has_valid_keys_valid(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_keys(message_data)
    assert valid


@pytest.mark.parametrize('message_data', [
    {'at': '2025-03-10T15:45:54.600066',
     'site': '4', 'val': 3},
    {'at': '2025-03-10T15:45:54.60',
     'site': '4', 'val': -1, 'type': '0'},
    {'at': '2025-03-10t15:45:54.600066',
     'site': '4', 'val': 3},
    {'at': '15:45:54.600066+00:00',
     'site': '4', 'val': -1, 'type': '0'}
])
def test_has_valid_values_valid_invalid_at(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_values(message_data)
    assert not valid


@pytest.mark.parametrize('message_data', [
    {'at': '2025-03-10T08:44:54.600066+00:00',
     'site': '4', 'val': 3, 'extra': 100},
    {'at': '2025-03-10T07:45:54.600066+00:00',
     'site': '4', 'val': -1, 'type': '0', 'extra': 100},
    {'at': '2025-03-10T18:15:54.600066+00:00',
     'site': '4', 'val': -1, 'type': '0', 'extra': 100},
    {'at': '2025-03-10T20:15:54.600066+00:00',
     'site': '4', 'val': -1, 'type': '0', 'extra': 100},
])
def test_has_valid_values_time(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_values(message_data)
    assert not valid


@pytest.mark.parametrize('message_data', [
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '10', 'val': 3},
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '10', 'val': -1, 'type': '0'},
])
def test_has_valid_values_site(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_values(message_data)
    assert not valid


@pytest.mark.parametrize('message_data', [
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '1', 'val': -2},
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '1', 'val': 5},
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '1', 'val': '3'},
])
def test_has_valid_values_val(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_values(message_data)
    assert not valid


@pytest.mark.parametrize('message_data', [
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '1', 'val': -1, 'type': -2},
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '1', 'val': -1, 'type': 2},
    {'at': '2025-03-10T15:45:54.600066+00:00',
     'site': '1', 'val': -1, 'type': '0'},
])
def test_has_valid_values_type(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_values(message_data)
    assert not valid


@pytest.mark.parametrize('message_data', [
    {"at": "2025-03-10T15:44:34.590745+00:00", "site": "1", "val": 0},
    {"at": "2025-03-10T15:44:34.590745+00:00",
        "site": "2", "val": -1, "type": 0},
    {"at": "2025-03-10T15:44:34.590745+00:00",
        "site": "3", "val": -1, "type": 1},
    {"at": "2025-03-10T09:44:34.590745+00:00", "site": "1", "val": 0},
    {"at": "2025-03-10T12:44:34.590745+00:00", "site": "2", "val": 1},
    {"at": "2025-03-10T14:44:34.590745+00:00", "site": "3", "val": 2},
    {"at": "2025-03-10T16:44:34.590745+00:00", "site": "1", "val": 3},
    {"at": "2025-03-10T17:44:34.590745+00:00", "site": "2", "val": 2},
])
def test_has_valid_values_valid(mock_transformer, message_data):
    valid, _ = mock_transformer._has_valid_values(message_data)
    assert valid


def test_log_invalid_message(caplog, mock_transformer):
    issue = 'my special issue'
    with caplog.at_level(logging.INFO):
        mock_transformer._log_invalid_message(INVALID_MESSAGE_VALUE, issue)
    assert issue in caplog.text
    assert INVALID_MESSAGE_VALUE in caplog.text


def test_transform_rating(mock_transformer):
    kiosk_event = mock_transformer._transform(json.loads(RATING_MESSAGE_VALUE))
    assert kiosk_event.is_rating()
    assert kiosk_event._KioskEvent__event_at.year == 2025
    assert kiosk_event._KioskEvent__event_at.month == 3
    assert kiosk_event._KioskEvent__event_at.day == 10
    assert kiosk_event._KioskEvent__event_at.hour == 15
    assert kiosk_event._KioskEvent__event_at.minute == 45
    assert kiosk_event._KioskEvent__event_at.second == 54
    assert kiosk_event._KioskEvent__exhibition_id == 1
    assert kiosk_event._KioskEvent__rating_or_request_id == 5


def test_transform_request(mock_transformer):
    kiosk_event = mock_transformer._transform(
        json.loads(REQUEST_MESSAGE_VALUE))
    assert not kiosk_event.is_rating()
    assert kiosk_event._KioskEvent__event_at.year == 2025
    assert kiosk_event._KioskEvent__event_at.month == 3
    assert kiosk_event._KioskEvent__event_at.day == 10
    assert kiosk_event._KioskEvent__event_at.hour == 15
    assert kiosk_event._KioskEvent__event_at.minute == 45
    assert kiosk_event._KioskEvent__event_at.second == 54
    assert kiosk_event._KioskEvent__exhibition_id == 2
    assert kiosk_event._KioskEvent__rating_or_request_id == 2


def test_create_kiosk_event_not_json(mock_transformer):
    result = mock_transformer.create_kiosk_event("wow ok dwi")
    assert result is None


def test_create_kiosk_event_invalid(mock_transformer):
    result = mock_transformer.create_kiosk_event(INVALID_MESSAGE_VALUE)
    assert result is None


def test_create_kiosk_event_valid(mock_transformer):
    result = mock_transformer.create_kiosk_event(VALID_MESSAGE_VALUE)
    assert isinstance(result, KioskEvent)


def test_create_kiosk_event_invalid_no_logging(caplog, mock_transformer_without_logger):
    with caplog.at_level(logging.INFO):
        mock_transformer_without_logger.create_kiosk_event(
            INVALID_MESSAGE_VALUE)
    assert INVALID_MESSAGE_VALUE not in caplog.text
