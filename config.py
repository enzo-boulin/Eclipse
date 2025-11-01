from enum import Enum

# Constants
RTE_BASE_URL: str = "https://digital.iservices.rte-france.com/"


RTE_CONSUMPTION_CLIENT_ID: str = "6a4825cb-10b9-4759-93f9-8f946879e212"
RTE_CONSUMPTION_CLIENT_SECRET: str = "7203ec04-a36e-49e5-b858-1af16e1562aa"

RTE_WHOLESALE_MARKET_CLIENT_ID = "8d4bd4a8-ba39-491c-8b6a-de907986b32e"
RTE_WHOLESALE_MARKET_CLIENT_SECRET = "6682163a-166c-47f6-a6cf-6872aa29f193"

WHOLESALE_MARKET_ENDPOINT = "open_api/wholesale_market/v3/france_power_exchanges"
CONSUMPTION_ENDPOINT = "open_api/consumption/v1/short_term"
TOKEN_ENDPOINT = "token/oauth/"


class APIName(str, Enum):
    wholesale_market = "wholesale_market"
    consumption = "consumption"


API_TO_CREDENTIALS = {
    APIName.wholesale_market: (
        RTE_WHOLESALE_MARKET_CLIENT_ID,
        RTE_WHOLESALE_MARKET_CLIENT_SECRET,
    ),
    APIName.consumption: (RTE_CONSUMPTION_CLIENT_ID, RTE_CONSUMPTION_CLIENT_SECRET),
}

ENDPOINT_TO_API = {
    WHOLESALE_MARKET_ENDPOINT: APIName.wholesale_market,
    CONSUMPTION_ENDPOINT: APIName.consumption,
}


class PrevisionType(str, Enum):
    REALISED = "REALISED"
    CORRECTED = "CORRECTED"
    ID = "ID"
    D_MINUS_1 = "D-1"
    D_MINUS_2 = "D-2"
