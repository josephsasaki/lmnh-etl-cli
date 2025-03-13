# pylint: skip-file

import pytest
from datetime import datetime

from lmnh_etl_cli.models.kiosk_event import KioskEvent


def test_kiosk_event_init():
    kiosk_event = KioskEvent(
        event_at=datetime(2025, 3, 10, 15, 44, 34, 590745),
        exhibition_id=2,
        is_rating=True,
        rating_or_request_id=10,
    )
    assert kiosk_event._KioskEvent__event_at.year == 2025
    assert kiosk_event._KioskEvent__event_at.month == 3
    assert kiosk_event._KioskEvent__event_at.day == 10
    assert kiosk_event._KioskEvent__event_at.hour == 15
    assert kiosk_event._KioskEvent__event_at.minute == 44
    assert kiosk_event._KioskEvent__event_at.second == 34
    assert kiosk_event._KioskEvent__event_at.microsecond == 590745
    assert kiosk_event._KioskEvent__exhibition_id == 2
    assert kiosk_event._KioskEvent__is_rating
    assert kiosk_event._KioskEvent__rating_or_request_id == 10


def test_get_insert_param():
    event_at = datetime(2025, 3, 10, 15, 44, 34, 590745)
    kiosk_event = KioskEvent(
        event_at=event_at,
        exhibition_id=2,
        is_rating=True,
        rating_or_request_id=10,
    )
    insert_param = kiosk_event.get_insert_param()
    assert insert_param[0] == 2
    assert insert_param[1] == 10
    assert insert_param[2] == event_at
    assert len(insert_param) == 3


def test_is_rating_true():
    event_at = datetime(2025, 3, 10, 15, 44, 34, 590745)
    kiosk_event = KioskEvent(
        event_at=event_at,
        exhibition_id=2,
        is_rating=True,
        rating_or_request_id=10,
    )
    assert kiosk_event.is_rating()


def test_is_rating_false():
    event_at = datetime(2025, 3, 10, 15, 44, 34, 590745)
    kiosk_event = KioskEvent(
        event_at=event_at,
        exhibition_id=2,
        is_rating=False,
        rating_or_request_id=10,
    )
    assert not kiosk_event.is_rating()
