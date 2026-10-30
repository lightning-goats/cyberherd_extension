"""Evening wording must not be selected just because daylight is cloudy."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from lnbits.extensions.cyberherd.services.evening import evening_context


@pytest.mark.parametrize(
    "hour,phase,sunset,direct,expected",
    [
        (12, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "0 W/m²", False),
        (16, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "0 W/m²", False),
        (18, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "0 W/m²", True),
        (18, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "15 W/m²", False),
        (19, "CIVIL_DUSK", None, None, True),
        (22, "NIGHT", None, None, True),
        (5, "NIGHT", None, None, False),
        (19, "DAYLIGHT", None, None, False),
        (19, None, None, "5 W/m²", False),
        (19, None, None, None, True),
        (16, None, None, None, False),
        (19, None, "2026-10-29T18:04:00-06:00", None, True),
        (18, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "NaN W/m²", False),
        (18, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "0 kW/m²", False),
        (18, "DAYLIGHT", "2026-10-30T18:04:00-06:00", "-1 W/m²", False),
        (18, "DAYLIGHT", "2026-10-30T18:04:00", "0 W/m²", False),
    ],
)
def test_evening_context(hour, phase, sunset, direct, expected):
    now = datetime(2026, 10, 30, hour, tzinfo=ZoneInfo("America/Denver"))
    assert evening_context(phase, sunset, direct, now=now) is expected
