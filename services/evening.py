"""Read-only solar context for evening feeder communications."""

import asyncio
import math
from datetime import datetime

from .time_utils import _get_system_local_timezone

ITEMS = ("Sun_SunPhaseName", "Sun_Set_Start", "Sun_DirectRadiation")
DUSK = {"SUN_SET", "CIVIL_DUSK", "NAUTIC_DUSK", "ASTRO_DUSK", "NIGHT"}
DAY = {"SUN_RISE", "ASTRO_DAWN", "NAUTIC_DAWN", "CIVIL_DAWN", "DAYLIGHT", "NOON"}


def evening_context(phase, sunset, direct, *, now):
    """Low sunlight alone must never turn a daytime pause into a night sign-off."""
    if now.hour < 15:
        return False
    if phase in DUSK:
        return True
    try:
        sunset_at = datetime.fromisoformat(sunset)
        if (
            sunset_at.tzinfo is None
            or sunset_at.astimezone(now.tzinfo).date() != now.date()
        ):
            raise ValueError("Not today's sunset")
    except (TypeError, ValueError):
        sunset_at = None
    radiation = _radiation(direct)
    if sunset_at:
        seconds_to_sunset = (sunset_at - now).total_seconds()
        if seconds_to_sunset <= 0:
            return True
        if seconds_to_sunset <= 3600 and radiation is not None and radiation <= 0.5:
            return True
        return False
    # Respect known daylight over a clock fallback, especially during summer.
    if phase in DAY or (radiation is not None and radiation > 0.5):
        return False
    return now.hour >= 18


def _radiation(direct):
    try:
        value, unit = direct.split()
        radiation = float(value)
        if (
            unit not in ("W/m²", "W/m2")
            or not math.isfinite(radiation)
            or radiation < 0
        ):
            raise ValueError("Invalid radiation")
    except (AttributeError, TypeError, ValueError):
        return None
    return radiation


async def is_evening_pause(client, *, now=None):
    """Missing Items fall back conservatively; no OpenHAB commands are sent."""
    now = now or datetime.now(_get_system_local_timezone())
    values = await asyncio.gather(
        *(client.get_item_state(item) for item in ITEMS), return_exceptions=True
    )
    values = [value if isinstance(value, str) else None for value in values]
    return evening_context(*values, now=now)
