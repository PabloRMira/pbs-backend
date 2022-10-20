import pandas as pd

from pbs.etl.preparation import prepare_bis_features


def test_prepare_bis_features():
    df = pd.DataFrame(
        {
            "time_index": ["2012", "2014", "2020"],
            "Australien - asdf": [1, 2, 3],
            "Spain - asdf": [10, 20, 30],
        }
    )
    countries = ["Australien", "Spain"]
    field_identifier = " - asdf"
    feature_name = "credit"
    time_colname = "time_index"
    country_map = {"Australien": "Australia", "Spain": "Spain"}
    out = prepare_bis_features(
        df, countries, field_identifier, feature_name, time_colname, country_map
    )
    expected = pd.DataFrame(
        {
            "country": ["Australia"] * 3 + ["Spain"] * 3,
            "time": ["2012", "2014", "2020"] * 2,
            "credit": [1, 2, 3, 10, 20, 30],
        }
    )
    pd.testing.assert_frame_equal(out, expected)
