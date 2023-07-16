import pytest

from src.algorithms.array_based_algorithm import calculate_heat_wave, HeatwaveHelper
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


@pytest.fixture
def heatwave_helper():
    return HeatwaveHelper()


def test_when_heatwave_helper_is_instantiated_then_all_fields_are_zero(heatwave_helper):
    assert heatwave_helper.start_index == 0
    assert heatwave_helper.end_index == 0
    assert heatwave_helper.number_of_days == 0
    assert heatwave_helper.number_of_tropical_days == 0
    assert heatwave_helper.max_temp == 0.0


def test_when_fields_constitute_heat_wave_then_true(heatwave_helper):
    heatwave_helper.number_of_days = 5
    heatwave_helper.number_of_tropical_days = 3

    assert heatwave_helper.is_heatwave() is True


@pytest.mark.parametrize("number_days,tropical_days", [(4, 3), (5, 2)])
def test_when_fields_do_not_constitute_heat_wave_then_false(
    heatwave_helper, number_days, tropical_days
):
    heatwave_helper.number_of_days = number_days
    heatwave_helper.number_of_tropical_days = tropical_days

    assert heatwave_helper.is_heatwave() is False
