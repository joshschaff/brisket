from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path

CT = timezone(timedelta(hours=-6))  # todo: dst
"""central time zone"""


class GridStatusScedDatasets(StrEnum):
    # TODO: is this really necessary? or just hard code strings inside member functions?
    ERCOT_SHADOW_PRICES_SCED = 'ercot_shadow_prices_sced'
    ERCOT_SCED_GEN_RESOURCE_60_DAY = 'ercot_sced_gen_resource_60_day'
    ERCOT_SCED_SYSTEM_LAMBDA = 'ercot_sced_system_lambda'
    ERCOT_LMP_BY_BUS = 'ercot_lmp_by_bus'
    ERCOT_LMP_BY_SETTLEMENT_POINT = 'ercot_lmp_by_settlement_point'
