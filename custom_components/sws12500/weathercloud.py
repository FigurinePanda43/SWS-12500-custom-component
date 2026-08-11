"""Weathercloud resend functions.

Weathercloud (https://app.weathercloud.net) expects metric values multiplied
by ten, so every value received from the station has to be converted first.
See https://weathercloud.net/en/api for the protocol description.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
import logging
from typing import Any, Literal

from aiohttp import ClientError

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    WEATHERCLOUD_BAD_REQUEST,
    WEATHERCLOUD_ENABLED,
    WEATHERCLOUD_ID,
    WEATHERCLOUD_INVALID_KEY,
    WEATHERCLOUD_KEY,
    WEATHERCLOUD_LOGGER_ENABLED,
    WEATHERCLOUD_SEND_DEFAULT,
    WEATHERCLOUD_SEND_INTERVAL,
    WEATHERCLOUD_SUCCESS,
    WEATHERCLOUD_TOO_MANY,
    WEATHERCLOUD_UNEXPECTED,
    WEATHERCLOUD_URL,
)
from .utils import fahrenheit_to_celsius, update_options

_LOGGER = logging.getLogger(__name__)

INHG_TO_HPA = 33.86389
MPH_TO_MS = 0.44704
INCH_TO_MM = 25.4


def _scaled(value: float) -> int:
    """Return metric value as expected by Weathercloud (value * 10)."""
    return round(value * 10)


def _plain(value: float) -> int:
    """Return value with no conversion (humidity, wind direction)."""
    return round(value)


def _fahrenheit(value: float) -> int:
    """Convert Fahrenheit to Celsius * 10."""
    return _scaled(fahrenheit_to_celsius(value))


def _inhg(value: float) -> int:
    """Convert inches of mercury to hPa * 10."""
    return _scaled(value * INHG_TO_HPA)


def _mph(value: float) -> int:
    """Convert miles per hour to m/s * 10."""
    return _scaled(value * MPH_TO_MS)


def _inch(value: float) -> int:
    """Convert inches to mm * 10."""
    return _scaled(value * INCH_TO_MM)


# Station key -> (Weathercloud keys, converter)
REMAP_WU_TO_WEATHERCLOUD: dict[str, tuple[tuple[str, ...], Callable[[float], int]]] = {
    "tempf": (("temp",), _fahrenheit),
    "indoortempf": (("tempin",), _fahrenheit),
    "dewptf": (("dew",), _fahrenheit),
    "humidity": (("hum",), _plain),
    "indoorhumidity": (("humin",), _plain),
    "baromin": (("bar",), _inhg),
    "windspeedmph": (("wspd", "wspdavg"), _mph),
    "windgustmph": (("wspdhi",), _mph),
    "winddir": (("wdir", "wdiravg"), _plain),
    "dailyrainin": (("rain",), _inch),
    "rainin": (("rainrate",), _inch),
    "solarradiation": (("solarrad",), _scaled),
    "UV": (("uvi",), _scaled),
}

REMAP_WSLINK_TO_WEATHERCLOUD: dict[
    str, tuple[tuple[str, ...], Callable[[float], int]]
] = {
    "t1tem": (("temp",), _scaled),
    "intem": (("tempin",), _scaled),
    "t1dew": (("dew",), _scaled),
    "t1hum": (("hum",), _plain),
    "inhum": (("humin",), _plain),
    "rbar": (("bar",), _scaled),
    "t1ws": (("wspd", "wspdavg"), _scaled),
    "t1wgust": (("wspdhi",), _scaled),
    "t1wdir": (("wdir", "wdiravg"), _plain),
    "t1raindy": (("rain",), _scaled),
    "t1rainra": (("rainrate",), _scaled),
    "t1solrad": (("solarrad",), _scaled),
    "t1uvi": (("uvi",), _scaled),
    "t1chill": (("chill",), _scaled),
    "t1heat": (("heat",), _scaled),
}


class WeathercloudBadRequest(Exception):
    """Weathercloud refused the payload."""


class WeathercloudSuccess(Exception):
    """Weathercloud accepted the data."""


class WeathercloudApiKeyError(Exception):
    """Weathercloud ID / Key error."""


class WeathercloudTooManyRequests(Exception):
    """Data sent more often than Weathercloud allows."""


def remap_to_weathercloud(
    data: dict[str, Any], mode: Literal["WU", "WSLINK"]
) -> dict[str, str]:
    """Convert station data to the Weathercloud payload."""

    remap = REMAP_WU_TO_WEATHERCLOUD if mode == "WU" else REMAP_WSLINK_TO_WEATHERCLOUD
    payload: dict[str, str] = {}

    for item, value in data.items():
        if item not in remap:
            continue

        try:
            _value = float(value)
        except (TypeError, ValueError):
            _LOGGER.debug("Skipping non numeric value for %s: %s", item, value)
            continue

        keys, convert = remap[item]
        for key in keys:
            payload[key] = str(convert(_value))

    return payload


class WeathercloudPush:
    """Push data to Weathercloud."""

    def __init__(self, hass: HomeAssistant, config: ConfigEntry) -> None:
        """Init."""
        self.hass = hass
        self.config = config
        self._interval = int(
            self.config.options.get(
                WEATHERCLOUD_SEND_INTERVAL, WEATHERCLOUD_SEND_DEFAULT
            )
        )

        self.last_update = datetime.now()
        self.next_update = datetime.now() + timedelta(seconds=self._interval)

        self.log = self.config.options.get(WEATHERCLOUD_LOGGER_ENABLED)
        self.invalid_response_count = 0

    def verify_response(self, response: str) -> None:
        """Verify answer from Weathercloud.

        Weathercloud answers with a plain HTTP status code in the body.
        """

        if self.log:
            _LOGGER.info("Weathercloud raw response: %s", response)

        _response = response.strip()

        if _response == "200":
            raise WeathercloudSuccess

        if _response == "400":
            raise WeathercloudBadRequest

        if _response in {"401", "403"}:
            raise WeathercloudApiKeyError

        if _response == "429":
            raise WeathercloudTooManyRequests

        raise WeathercloudBadRequest

    async def push_data_to_weathercloud(
        self, data: dict[str, Any], mode: Literal["WU", "WSLINK"]
    ):
        """Push weather data to Weathercloud."""

        if self.log:
            _LOGGER.info(
                "Weathercloud last update = %s, next update at: %s",
                str(self.last_update),
                str(self.next_update),
            )

        if self.next_update > datetime.now():
            _LOGGER.debug(
                "Triggered update interval limit of %s seconds. Next possible update is set to: %s",
                self._interval,
                self.next_update,
            )
            return False

        payload = remap_to_weathercloud(data, mode)

        if not payload:
            _LOGGER.warning(
                "No usable data for Weathercloud in the received dataset. Nothing sent"
            )
            return False

        payload["wid"] = self.config.options.get(WEATHERCLOUD_ID)
        payload["key"] = self.config.options.get(WEATHERCLOUD_KEY)

        session = async_get_clientsession(self.hass)

        if self.log:
            _LOGGER.info("Dataset for Weathercloud [mode=%s]: %s", mode, payload)

        try:
            async with session.get(WEATHERCLOUD_URL, params=payload) as resp:
                status = await resp.text()
                try:
                    self.verify_response(status)

                except WeathercloudSuccess:
                    self.invalid_response_count = 0
                    if self.log:
                        _LOGGER.info(WEATHERCLOUD_SUCCESS)

                except WeathercloudApiKeyError:
                    # log despite of settings
                    _LOGGER.critical(WEATHERCLOUD_INVALID_KEY)
                    await update_options(
                        self.hass, self.config, WEATHERCLOUD_ENABLED, False
                    )

                except WeathercloudTooManyRequests:
                    _LOGGER.warning(WEATHERCLOUD_TOO_MANY)

                except WeathercloudBadRequest:
                    _LOGGER.error("%s (response: %s)", WEATHERCLOUD_BAD_REQUEST, status)

        except ClientError as ex:
            _LOGGER.critical("Invalid response from Weathercloud: %s", str(ex))
            self.invalid_response_count += 1
            if self.invalid_response_count > 3:
                _LOGGER.critical(WEATHERCLOUD_UNEXPECTED)
                await update_options(
                    self.hass, self.config, WEATHERCLOUD_ENABLED, False
                )

        self.last_update = datetime.now()
        self.next_update = self.last_update + timedelta(seconds=self._interval)

        if self.log:
            _LOGGER.info("Next update: %s", str(self.next_update))

        return None
