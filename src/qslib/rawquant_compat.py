# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

import pandas as pd


def _fdc_to_rawdata(fdc: pd.DataFrame, start_time: float) -> pd.DataFrame:
    ret: pd.DataFrame = fdc.loc[:, (slice(None), "fl")].copy()
    ret.columns = [f"{r}{c}" for r in "ABCDEFGH" for c in range(1, 13)]
    for i in range(0, 6):
        ret[f"temperature_{i+1}"] = fdc[f"A{(i+1)*2}", "rt"]
    ret["temperature_avg"] = ret.loc[:, slice("temperature_1", "temperature_6")].mean(
        axis=1
    )
    ret["timestamp"] = fdc["time", "timestamp"]
    ret["exptime"] = ret["timestamp"] - start_time
    ret["exphrs"] = (ret["timestamp"] - start_time) / 60 / 60
    # ret['time'] = ret['time'].astype('datetime64[s]') # fixme: should we?

    return ret
