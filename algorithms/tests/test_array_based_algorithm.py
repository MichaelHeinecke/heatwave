import pytest

from src.algorithms.array_based_algorithm import calculate_heat_wave
from datetime import date, timedelta


@pytest.fixture
def dates_generator():
    """Return function to generate dates list of appropriate length per test case."""

    def _generate_dates(length):
        start_date = date(2004, 6, 1)
        return [str(start_date + timedelta(days=offset)) for offset in range(length)]

    return _generate_dates


def test_when_heat_wave_in_input_then_heat_wave_is_found(dates_generator):
    temperatures = [21.0, 25.9, 26.1, 30.6, 30.0, 25.3, 31.8, 25.1, 22.1, 25.4]
    dates = dates_generator(len(temperatures))
    expected_result = [("2004-06-02", "2004-06-08", 7, 3, 31.8)]

    assert calculate_heat_wave(dates, temperatures) == expected_result


def test_when_multiple_heat_waves_in_input_then_all_are_found(dates_generator):
    temperatures = [
        21.0,
        25.9,
        26.1,
        30.6,
        30.0,
        25.3,
        31.8,
        25.1,
        22.1,
        25.4,
        21.0,
        22.0,
        26.1,
        30.6,
        30.0,
        25.3,
        34.1,
        25.1,
        22.1,
        25.4,
    ]
    dates = dates_generator(len(temperatures))
    expected_result = [
        ("2004-06-02", "2004-06-08", 7, 3, 31.8),
        ("2004-06-13", "2004-06-18", 6, 3, 34.1),
    ]

    assert calculate_heat_wave(dates, temperatures) == expected_result


def test_when_no_heat_wave_in_input_then_empty_list_is_returned(dates_generator):
    temperatures = [21.0, 25.9, 26.1, 30.6, 30.0, 22.1, 31.8, 25.1, 22.1, 25.4]
    dates = dates_generator(len(temperatures))
    expected_result = []

    assert calculate_heat_wave(dates, temperatures) == expected_result


def test_when_inputs_are_not_of_equal_length_then_raise_value_error(dates_generator):
    temperatures = [21.3]
    dates = dates_generator(2)
    with pytest.raises(ValueError, match="The input lists must be of equal length."):
        calculate_heat_wave(dates, temperatures)
