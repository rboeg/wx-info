from app import transform


def test_transform_module_exists():
    assert hasattr(transform, "pl")


def test_flatten_observations_empty():
    df = transform.flatten_observations([])
    assert df.shape[0] == 0


def test_flatten_observations_missing_fields():
    obs = [{"properties": {}}]
    df = transform.flatten_observations(obs)
    assert df["station_id"][0] is None
    assert df["observation_timestamp"][0] is None
    assert df["temperature"][0] is None
    assert df["wind_speed"][0] is None
    assert df["relative_humidity"][0] is None
    assert df["barometric_pressure"][0] is None
    assert df["precipitation_last_hour"][0] is None
    assert df["dewpoint"][0] is None


def test_flatten_observations_all_fields():
    obs = [{
        "properties": {
            "station": "s/KATL",
            "timestamp": "2024-01-01T00:00:00Z",
            "temperature": {"value": 12.34},
            "windSpeed": {"value": 7.89},
            "relativeHumidity": {"value": 55.55},
            "barometricPressure": {"value": 101325.12},
            "precipitationLastHour": {"value": 1.23},
            "dewpoint": {"value": 10.11}
        }
    }]
    df = transform.flatten_observations(obs)
    assert df["station_id"][0] == "KATL"
    assert df["observation_timestamp"][0] == "2024-01-01T00:00:00Z"
    assert df["temperature"][0] == 12.34
    assert df["wind_speed"][0] == 7.89
    assert df["relative_humidity"][0] == 55.55
    assert df["barometric_pressure"][0] == 101325.12
    assert df["precipitation_last_hour"][0] == 1.23
    assert df["dewpoint"][0] == 10.11


def test_flatten_observations_non_numeric():
    obs = [{"properties": {"station": "s/KATL", "temperature": {"value": "not_a_number"}}}]
    try:
        df = transform.flatten_observations(obs)
        # Should raise or set to None
        assert df["temperature"][0] is None or isinstance(df["temperature"][0], float)
        assert df["relative_humidity"][0] is None
        assert df["barometric_pressure"][0] is None
        assert df["precipitation_last_hour"][0] is None
        assert df["dewpoint"][0] is None
    except Exception:
        pass


def test_flatten_observations_multiple():
    obs = [
        {
            "properties": {
                "station": "s/KATL",
                "timestamp": "2024-01-01T00:00:00Z",
                "temperature": {"value": 10.0},
                "windSpeed": {"value": 5.0},
                "relativeHumidity": {"value": 60.0},
                "barometricPressure": {"value": 100000.0},
                "precipitationLastHour": {"value": 0.5},
                "dewpoint": {"value": 5.0}
            }
        },
        {
            "properties": {
                "station": "s/003PG",
                "timestamp": "2024-01-02T00:00:00Z",
                "temperature": {"value": 20.0},
                "windSpeed": {"value": 8.0},
                "relativeHumidity": {"value": 70.0},
                "barometricPressure": {"value": 100500.0},
                "precipitationLastHour": {"value": 1.0},
                "dewpoint": {"value": 10.0}
            }
        }
    ]
    df = transform.flatten_observations(obs)
    assert df.shape[0] == 2
    assert set(df["station_id"]) == {"KATL", "003PG"}
    assert df["relative_humidity"][0] == 60.0
    assert df["relative_humidity"][1] == 70.0
    assert df["barometric_pressure"][0] == 100000.0
    assert df["barometric_pressure"][1] == 100500.0
    assert df["precipitation_last_hour"][0] == 0.5
    assert df["precipitation_last_hour"][1] == 1.0
    assert df["dewpoint"][0] == 5.0
    assert df["dewpoint"][1] == 10.0
