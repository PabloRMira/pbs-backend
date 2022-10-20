from typing import Dict, List

import pandas as pd

from pbs.config import ETLConfig


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
    cols = cols_map.values()
    return (
        df.rename(columns=cols_map)[cols]
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


def prepare_crisis_indicator(df: pd.DataFrame) -> pd.DataFrame:
    pass
