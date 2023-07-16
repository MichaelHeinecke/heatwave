from typing import List, Tuple
from dataclasses import dataclass


@dataclass
class HeatwaveHelper:
    """Helper class for keeping track of fields for calculating heat waves.

    The class comes with a utility method to check if the fields constitute a
    heat wave.
    """

    start_index: int = 0
    end_index: int = 0
    number_of_days: int = 0
    number_of_tropical_days: int = 0
    max_temp: float = 0.0

    def is_heatwave(self) -> bool:
        if self.number_of_days >= 5 and self.number_of_tropical_days >= 3:
            return True
        else:
            return False


def calculate_heat_wave(dates: List[str], temperatures: List[float]) -> List[Tuple]:
    """Calculate heat waves based on a list of dates and a list of corresponding
    temperatures.

    A heat wave is defined by the KNMI definition:
    A heat wave is a succession of at least 5 summer days (maximum temperature
    25.0 °C or higher) in De Bilt, of which at least three are tropical days
    (maximum temperature 30.0 °C or higher).

    :param dates: A list of dates.
    :param temperatures: A list of temperatures.
    :return: List of heat wave tuples. Each tuple has the format:
    (start_date, end_date, heat_wave_duration_in_days, number_of_tropical_days,
    maximum_temperature_during_heat_wave)
    :raises ValueError: Thrown if dates and temperatures lists are not of equal
    length.
    """
    if len(dates) != len(temperatures):
        raise ValueError("The input lists must be of equal length.")

    heatwaves = []
    helper = HeatwaveHelper()

    for i in range(0, len(temperatures)):
        if temperatures[i] >= 25.0:
            helper.number_of_days += 1
            helper.end_index = i
            if helper.start_index == 0:
                helper.start_index = i

        if temperatures[i] >= 30.0:
            helper.number_of_tropical_days += 1

        if temperatures[i] > helper.max_temp:
            helper.max_temp = temperatures[i]

        # If sequence of days with maximum temperatures of at least 25 is interrupted,
        # or we reach the end of the input array, check if we found a heat wave.
        if temperatures[i] < 25.0 or i == len(temperatures) - 1:
            if helper.is_heatwave():
                heatwaves.append(
                    (
                        dates[helper.start_index],
                        dates[helper.end_index],
                        helper.number_of_days,
                        helper.number_of_tropical_days,
                        helper.max_temp,
                    )
                )

            # Reset variables to check next potential heatwave.
            helper = HeatwaveHelper()

    return heatwaves
