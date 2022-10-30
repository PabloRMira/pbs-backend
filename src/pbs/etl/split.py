import datetime
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

from pbs.config import ETLConfig


def random_split(df: pd.DataFrame):
    splits = df[df["housing_crisis"] != 2].pipe(
        lambda df: train_test_split(
            df[ETLConfig.ID_COLUMNS],
            df[ETLConfig.RESPONSE],
            test_size=0.2,
            stratify=df[[ETLConfig.RESPONSE, "country"]],
        )
    )
    return pd.concat(
        [splits[0].assign(split="training"), splits[1].assign(split="test")], axis=0
    ).assign(split_type="random")


def time_split(df: pd.DataFrame):
    return df[df["housing_crisis"] != 2][ETLConfig.ID_COLUMNS].assign(
        split=lambda df: df["time"].map(
            lambda value: "training"
            if value <= datetime.datetime.fromisoformat(ETLConfig.TEST_SPLIT_DATE)
            else "test"
        ),
        split_type="time",
    )


def split(df: pd.DataFrame):
    return pd.concat([random_split(df), time_split(df)], axis=0)


def split_data(input_path: str, output_path: str):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pd.read_parquet(input_path).pipe(split).to_parquet(output_path, index=False)
