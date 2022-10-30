from dataclasses import dataclass


@dataclass
class ETLConfig:
    CREDIT_COUNTRIES = [
        "Argentina",
        "Austria",
        "Australia",
        "Belgium",
        "Brazil",
        "Canada",
        "Switzerland",
        "Chile",
        "China",
        "Colombia",
        "Czechia",
        "Germany",
        "Denmark",
        "Spain",
        "Finland",
        "France",
        "United Kingdom",
        "Greece",
        "Hungary",
        "Indonesia",
        "Ireland",
        "India",
        "Italy",
        "Japan",
        "Korea",
        "Luxembourg",
        "Mexico",
        "Malaysia",
        "Netherlands",
        "Norway",
        "New Zealand",
        "Poland",
        "Portugal",
        "Russia",
        "Sweden",
        "Singapore",
        "Thailand",
        "Turkey",
        "United States",
        "South Africa",
    ]
    CREDIT_IDENTIFIER = (
        " - Credit to Private non-financial sector from Banks, "
        "total at Market value - Domestic currency - Adjusted for breaks"
    )
    CREDIT_TO_GDP_IDENTIFIER = (
        " - Credit to Private non-financial sector from Banks, "
        "total at Market value - Percentage of GDP - Adjusted for breaks"
    )
    CREDIT_COUNTRY_MAP = {"Czechia": "Czech Republic"}
    HPI_COUNTRIES = [
        "Australia",
        "Belgium",
        "Canada",
        "Switzerland",
        "Germany",
        "Denmark",
        "Spain",
        "Finland",
        "France",
        "United Kingdom",
        "Ireland",
        "Italy",
        "Japan",
        "Korea",
        "Malaysia",
        "Netherlands",
        "Norway",
        "New Zealand",
        "Sweden",
        "Thailand",
        "United States",
        "South Africa",
    ]
    OECD_COUNTRY_MAP = {
        "AUS": "Australia",
        "AUT": "Austria",
        "BEL": "Belgium",
        "CAN": "Canada",
        "CHL": "Chile",
        "CZE": "Czech Republic",
        "DNK": "Denmark",
        "EST": "Estonia",
        "FIN": "Finland",
        "FRA": "France",
        "DEU": "Germany",
        "GRC": "Greece",
        "HUN": "Hungary",
        "ISL": "Island",
        "IRL": "Ireland",
        "ISR": "Israel",
        "ITA": "Italy",
        "JPN": "Japan",
        "KOR": "Korea",
        "LVA": "Latvia",
        "LUX": "Luxembourg",
        "MEX": "Mexico",
        "NLD": "Netherlands",
        "NZL": "New Zealand",
        "NOR": "Norway",
        "POL": "Poland",
        "PRT": "Portugal",
        "SVK": "Slovakia",
        "SVN": "Slovenia",
        "ESP": "Spain",
        "SWE": "Sweden",
        "CHE": "Switzerland",
        "TUR": "Turkey",
        "GBR": "United Kingdom",
        "USA": "United States",
        "ZAF": "South Africa",
        "CHN": "China",
        "COL": "Colombia",
        "IND": "India",
        "IDN": "Indonesia",
        "LTU": "Lithuania",
        "RUS": "Russia",
        "BRA": "Brazil",
    }
    CRISIS_QUARTER_LENGTH = 20
    PRECRISIS_QUARTER_LENGTH = 12
    RESPONSE = "housing_crisis"
    TEST_SPLIT_DATE = "2001-01-31"
    ID_COLUMNS = ["country", "time"]
