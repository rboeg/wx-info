import pytest
from pipeline import WeatherPipeline

def test_pipeline_init():
    pipeline = WeatherPipeline('KATL', 'postgresql://user:pass@localhost/db')
    assert pipeline.station_id == 'KATL' 