import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from requests.exceptions import HTTPError

from pbs.config import ETLConfig
from pbs.logger import log_step, logger
from pbs.utils import get_oecd_response


def _fetch_bis_data(url: str, skiprows: Optional[List[int]] = None) -> pd.DataFrame:
    response = requests.get(url, stream=True)
    if not response.ok:
        raise requests.exceptions.HTTPError("Data fetching failed!")
    with tempfile.TemporaryFile(mode="rb+") as f:
        f.write(response.content)
        return pd.read_excel(io=f, sheet_name="Quarterly Series", skiprows=skiprows)


def fetch_credit_data() -> pd.DataFrame:
    url = "http://www.bis.org/statistics/totcredit/totcredit.xlsx"
    return _fetch_bis_data(url=url, skiprows=[1, 2, 3])


def fetch_hpi_data() -> pd.DataFrame:
    url = "http://www.bis.org/statistics/pp/pp_long.xlsx"
    return _fetch_bis_data(url=url, skiprows=[0, 1, 3])


def get_oecd_url(
    root_url: str,
    countries: List[str],
    database: str,
    indicator: str,
    measurement: Optional[str],
    frequency: str,
):
    countries_url_param = "+".join(countries)
    dimensions_params = (
        [indicator, countries_url_param, frequency]
        if database == "MEI_FIN"
        else [countries_url_param, indicator] + [measurement, frequency]  # type: ignore
    )
    dimensions = [dimension for dimension in dimensions_params if dimension is not None]
    dimensions_url_param = ".".join(dimensions)
    return f"{root_url}/{database}/{dimensions_url_param}/all"


def fetch_oecd_data(
    database: str,
    country_map: Dict[str, str],
    indicator: str,
    measurement: Optional[str],
    frequency: str,
    feature_name: str,
):
    """Get OECD from OECD API

    API documentation: https://data.oecd.org/api/sdmx-json-documentation/#d.en.330346

    :param database: OECD database
    :param country_map: Country mapping of the form {"oecd_country_name": "custom country name"}
    :param indicator: Identifier for economic indicator
    :param measurement: Identifier for measurement
    :param frequency: Frequency identifier
    :param feature_name: Name for the feature in the final dataset
    """
    oecd_countries = list(country_map.keys())
    countries = list(country_map.values())
    url = get_oecd_url(
        "http://stats.oecd.org/SDMX-JSON/data",
        oecd_countries,
        database,
        indicator,
        measurement,
        frequency,
    )
    response = get_oecd_response(url)
    if response.status_code != 200:
        raise HTTPError(response.text)
    data = response.json()
    time = pd.to_datetime(
        [record["id"] for record in data["structure"]["dimensions"]["observation"][0]["values"]]
    ) + pd.offsets.QuarterEnd(0)
    return pd.concat(
        [
            pd.DataFrame(
                {
                    "country": countries[idx],
                    "time": time[[int(record) for record in value["observations"].keys()]],
                    feature_name: [record[0] for record in value["observations"].values()],
                }
            )
            for idx, value in enumerate(list(data["dataSets"][0]["series"].values()))
        ],
        axis=0,
    )


def fetch_cpi() -> pd.DataFrame:
    return fetch_oecd_data(
        database="PRICES_CPI",
        country_map=ETLConfig.OECD_COUNTRY_MAP,
        indicator="CPALTT01",
        measurement="IXOB",
        frequency="Q",
        feature_name="cpi",
    )


def fetch_ishort() -> pd.DataFrame:
    return fetch_oecd_data(
        database="MEI_FIN",
        country_map=ETLConfig.OECD_COUNTRY_MAP,
        indicator="IR3TIB",
        measurement=None,
        frequency="Q",
        feature_name="ishort",
    )


def fetch_ilong() -> pd.DataFrame:
    return fetch_oecd_data(
        database="MEI_FIN",
        country_map=ETLConfig.OECD_COUNTRY_MAP,
        indicator="IRLT",
        measurement=None,
        frequency="Q",
        feature_name="ilong",
    )


@log_step
def extract_data(output_dir: str):
    output_dirpath = Path(output_dir)
    output_dirpath.mkdir(parents=True, exist_ok=True)
    extractions = {
        "credit": fetch_credit_data,
        "hpi": fetch_hpi_data,
        "cpi": fetch_cpi,
        "ishort": fetch_ishort,
        "ilong": fetch_ilong,
    }
    for name, func in extractions.items():
        logger.info(f"Extracting {name} data")
        output_path = output_dirpath.joinpath(f"{name}.parquet")
        func().to_parquet(output_path, index=False)
