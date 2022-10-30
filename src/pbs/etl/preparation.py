import functools
import itertools
from pathlib import Path
from typing import Dict, List

import pandas as pd

from pbs.config import ETLConfig
from pbs.logger import log_step


def prepare_bis_features(
    df: pd.DataFrame,
    countries: List[str],
    field_identifier: str,
    feature_name: str,
    time_colname: str,
    country_map: Dict[str, str] = {},
) -> pd.DataFrame:
    cols_map = {
        time_colname: "time",
        **{country + field_identifier: country for country in countries},
    }
    return (
        df.rename(columns=cols_map)[cols_map.values()]
        .melt(
            id_vars="time",
            value_vars=countries,
            var_name="country",
            value_name=feature_name,
        )[["country", "time", feature_name]]
        .assign(
            country=lambda df: df["country"].map(
                lambda x: country_map[x] if x in country_map.keys() else x
            )
        )
        .sort_values(by=["country", "time"])
    )


def prepare_credit_data(df: pd.DataFrame):
    return prepare_bis_features(
        df,
        ETLConfig.CREDIT_COUNTRIES,
        ETLConfig.CREDIT_IDENTIFIER,
        "credit",
        "Back to menu",
        ETLConfig.CREDIT_COUNTRY_MAP,
    )


def prepare_credit_to_gdp_data(df: pd.DataFrame):
    return prepare_bis_features(
        df,
        ETLConfig.CREDIT_COUNTRIES,
        ETLConfig.CREDIT_TO_GDP_IDENTIFIER,
        "credit_to_gdp",
        "Back to menu",
        ETLConfig.CREDIT_COUNTRY_MAP,
    )


def prepare_hpi_data(df: pd.DataFrame):
    return prepare_bis_features(df, ETLConfig.HPI_COUNTRIES, "", "hpi", "Unnamed: 0")


def prepare_crises_data(df: pd.DataFrame):
    return pd.concat(
        df.assign(time=lambda df: pd.to_datetime(df["time"]).dt.to_period("Q").dt.end_time.dt.date)
        .apply(
            lambda row: pd.concat(
                [
                    pd.DataFrame(
                        {
                            "country": row["country"],
                            "time": pd.date_range(
                                start=row["time"]
                                + pd.offsets.QuarterEnd(-ETLConfig.PRECRISIS_QUARTER_LENGTH),
                                end=row["time"] + pd.offsets.QuarterEnd(-1),
                                freq="Q",
                            ),
                            "housing_crisis": row["housing_bubble"],
                            "financial_crisis": row["financial_crisis"],
                        }
                    ),
                    pd.DataFrame(
                        {
                            "country": row["country"],
                            "time": pd.date_range(
                                start=row["time"],
                                end=row["time"]
                                + pd.offsets.QuarterEnd(ETLConfig.CRISIS_QUARTER_LENGTH),
                                freq="Q",
                            ),
                            "housing_crisis": 2 if row["housing_bubble"] == 1 else 0,
                            "financial_crisis": 2 if row["financial_crisis"] == 1 else 0,
                        }
                    ),
                ],
                axis=0,
            ),
            axis=1,
        )
        .tolist(),
        axis=0,
    ).sort_values(by=["country", "time"])


@log_step
def prepare_data(input_dir: str, output_path: str):
    input_dirpath = Path(input_dir)
    output_path_ = Path(output_path)
    output_path_.parent.mkdir(parents=True, exist_ok=True)
    preparation_steps = {
        "credit": [prepare_credit_data, prepare_credit_to_gdp_data],
        "hpi": [prepare_hpi_data],
        "crises": [prepare_crises_data],
    }
    df = functools.reduce(
        lambda df1, df2: pd.merge(df1, df2, on=["country", "time"], how="outer"),
        itertools.chain.from_iterable(
            [
                [
                    pd.read_parquet(input_dirpath.joinpath(f"{name}.parquet")).pipe(func)
                    for func in funcs
                ]
                for name, funcs in preparation_steps.items()
            ]
        ),
    ).sort_values(by=["country", "time"], ascending=True)
    df.to_parquet(output_path, index=False)
