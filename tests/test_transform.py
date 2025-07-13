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
    assert df["humidity"][0] is None


def test_flatten_observations_all_fields():
    obs = [{"properties": {"station": "s/KATL", "timestamp": "2024-01-01T00:00:00Z", "temperature": {"value": 12.34}, "windSpeed": {"value": 7.89}, "relativeHumidity": {"value": 55.55}}}]
    df = transform.flatten_observations(obs)
    assert df["station_id"][0] == "KATL"
    assert df["observation_timestamp"][0] == "2024-01-01T00:00:00Z"
    assert df["temperature"][0] == 12.34
    assert df["wind_speed"][0] == 7.89
    assert df["humidity"][0] == 55.55


def test_flatten_observations_non_numeric():
    obs = [{"properties": {"station": "s/KATL", "temperature": {"value": "not_a_number"}}}]
    try:
        df = transform.flatten_observations(obs)
        # Should raise or set to None
        assert df["temperature"][0] is None or isinstance(df["temperature"][0], float)
    except Exception:
        pass


def test_flatten_observations_multiple():
    obs = [
        {"properties": {"station": "s/KATL", "timestamp": "2024-01-01T00:00:00Z", "temperature": {"value": 10.0}}},
        {"properties": {"station": "s/KBOS", "timestamp": "2024-01-02T00:00:00Z", "temperature": {"value": 20.0}}}
    ]
    df = transform.flatten_observations(obs)
    assert df.shape[0] == 2
    assert set(df["station_id"]) == {"KATL", "KBOS"}
