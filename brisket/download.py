from gridstatusio.gs_client import GridStatusClient
import pandas as pd

from datetime import datetime, timezone, timedelta
from enum import StrEnum
from pathlib import Path


class GridStatusScedDatasets(StrEnum):
    # TODO: is this really necessary? or just hard code strings inside member functions?
    ERCOT_SHADOW_PRICES_SCED = 'ercot_shadow_prices_sced'
    ERCOT_SCED_GEN_RESOURCE_60_DAY = 'ercot_sced_gen_resource_60_day'
    ERCOT_SCED_SYSTEM_LAMBDA = 'ercot_sced_system_lambda'
    ERCOT_LMP_BY_BUS = 'ercot_lmp_by_bus'
    ERCOT_LMP_BY_SETTLEMENT_POINT = 'ercot_lmp_by_settlement_point'


CT = timezone(timedelta(hours=-6))  # todo: dst

SCED_MINUTES = 5
SCED_TIMESTAMP_COLUMN_NAME = 'sced_timestamp_utc'


def time_floor(time, delta):
    """Floor a datetime object to a multiple of a timedelta."""
    mod = (time - datetime.min) % delta
    return time - mod


class GridStatusRepository():
    _client: GridStatusClient
    _data_path: Path

    def __init__(self, client: GridStatusClient, data_path: Path):
        self._client = client
        self._data_path = data_path

        # ensure all dataset directories exist
        for dataset in GridStatusScedDatasets:
            dataset_path = data_path / dataset
            dataset_path.mkdir(parents=True, exist_ok=True)

    def _get_missing_sced_dt_range(
            self,
            sced_dts: pd.Series,
            start_dt: datetime,
            end_dt: datetime) -> tuple[datetime, datetime]:
        """ Return the frist missing, last missing sced timestamp"""

        print(f'{sced_dts=}')

        start_missing_dt = start_dt
        end_missing_dt = start_dt

        sced_dt = start_dt
        while sced_dt < end_dt:
            next_sced_dt = sced_dt + timedelta(minutes=SCED_MINUTES)

            is_geq_sced_dt = sced_dts >= sced_dt
            is_lt_next_sced_dt = sced_dts < next_sced_dt
            if sced_dts[is_geq_sced_dt & is_lt_next_sced_dt].empty:
                start_missing_dt = min(start_missing_dt, sced_dt)
                end_missing_dt = max(end_missing_dt, next_sced_dt)
            
            sced_dt = next_sced_dt

        return start_missing_dt, end_missing_dt

    def _get_sced_data_from_cache(self, dataset: GridStatusScedDatasets, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        # get list of cached timestamps
        dataset_path = self._data_path / dataset

        dfs = []
        for entry in dataset_path.iterdir():
            if entry.is_file() and entry.suffix == '.csv':
                try:
                    sced_dt = datetime.fromisoformat(entry.stem)
                    if sced_dt >= start_dt and sced_dt < end_dt:
                        dfs.append(pd.read_csv(entry, parse_dates=[SCED_TIMESTAMP_COLUMN_NAME]))
                except Exception:
                    print(f'Failed to parse {entry} as dataset')
                    continue

        return pd.concat(dfs) if len(dfs) > 0 else pd.DataFrame({SCED_TIMESTAMP_COLUMN_NAME: []})

    def _get_sced_data_from_gridstatus(self, dataset: GridStatusScedDatasets, start_dt: datetime, end_dt: datetime):
        # ensure dataset path exists
        dataset_path = self._data_path / dataset
        dataset_path.mkdir(parents=True, exist_ok=True)
        # query from gridstatus
        data = self._client.get_dataset(dataset, start=start_dt, end=end_dt)
        # split by sced dt, save to directory
        for sced_ts, sced_ts_data in data.groupby(SCED_TIMESTAMP_COLUMN_NAME):
            sced_ts_path = dataset_path / f'{sced_ts.isoformat()}.csv'
            sced_ts_data.to_csv(sced_ts_path, index=False)
        # return unsplit data
        return data

    def _get_sced_data(self, dataset: GridStatusScedDatasets, start_dt: datetime, end_dt: datetime):
        """main function dispatched to from each dataset"""
        print('inside _get_sced_data')
        
        # TODO: round up / down to the nearest 5 minutes?

        # fetch cached data
        cached_data = self._get_sced_data_from_cache(dataset=dataset, start_dt=start_dt, end_dt=end_dt)
        print(f'{cached_data=}')


        # get missing range
        missing_start_dt, missing_end_dt = self._get_missing_sced_dt_range(
            sced_dts=cached_data[SCED_TIMESTAMP_COLUMN_NAME],
            start_dt=start_dt,
            end_dt=end_dt
        )
        print(f'{missing_start_dt=}')
        print(f'{missing_end_dt=}')



        # if all data is served from cache, return
        if missing_start_dt == missing_end_dt:
            return cached_data

        # otherwise, download missing data from gridstatus
        gridstatus_data = self._get_sced_data_from_gridstatus(dataset=dataset, start_dt=start_dt, end_dt=end_dt)

        # return cached data combined w/ downloaded data
        is_duplicate_cached_data = cached_data[SCED_TIMESTAMP_COLUMN_NAME].isin(gridstatus_data[SCED_TIMESTAMP_COLUMN_NAME])
        return pd.concat((
            cached_data[~is_duplicate_cached_data],
            gridstatus_data
        )).sort_values(by=SCED_TIMESTAMP_COLUMN_NAME)


    def get_ercot_shadow_prices_sced(self, start_dt, end_dt):
        return self._get_sced_data(
            dataset=GridStatusScedDatasets.ERCOT_SHADOW_PRICES_SCED,
            start_dt=start_dt,
            end_dt=end_dt)

    def get_ercot_sced_gen_resource_60_day(self, start_dt, end_dt):
        return self._get_sced_data(
            dataset=GridStatusScedDatasets.ERCOT_SCED_GEN_RESOURCE_60_DAY,
            start_dt=start_dt,
            end_dt=end_dt)

    def get_ercot_sced_system_lambda(self, start_dt, end_dt):
        return self._get_sced_data(
            dataset=GridStatusScedDatasets.ERCOT_SCED_SYSTEM_LAMBDA,
            start_dt=start_dt,
            end_dt=end_dt)

    def get_ercot_lmp_by_bus(self, start_dt, end_dt):
        return self._get_sced_data(
            dataset=GridStatusScedDatasets.ERCOT_LMP_BY_BUS,
            start_dt=start_dt,
            end_dt=end_dt)
    
    def get_ercot_lmp_by_settlement_point(self, start_dt, end_dt):
        return self._get_sced_data(
            dataset=GridStatusScedDatasets.ERCOT_LMP_BY_SETTLEMENT_POINT,
            start_dt=start_dt,
            end_dt=end_dt)




