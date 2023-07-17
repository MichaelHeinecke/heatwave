package org.michaelheinecke.heatwave;

import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

public class HeatWaveTest {

  @Test
  void whenHeatWaveInInput_ThenWaveIsFound() {
    List<DailyTemperatureReading> inputList = List.of(
        new DailyTemperatureReading(LocalDate.of(2004, 6, 1), "", 21.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 2), "", 25.9),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 3), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 4), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 5), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 6), "", 25.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 7), "", 31.8),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 8), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 9), "", 22.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 10), "", 25.4)
    );
    List<HeatWave> actualResult = HeatWave.calculateHeatWaves(inputList);

    List<HeatWave> expectedResult =
        List.of(new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 2), 7, 3, 31.8));

    // TODO: add assert
  }

  @Test
  void whenMultiplesHeatWavesInInput_ThenAllFound() {
    List<DailyTemperatureReading> inputList = List.of(
        new DailyTemperatureReading(LocalDate.of(2004, 6, 1), "", 21.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 2), "", 25.9),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 3), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 4), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 5), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 6), "", 25.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 7), "", 31.8),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 8), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 9), "", 22.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 10), "", 25.4),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 11), "", 21.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 12), "", 22.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 13), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 14), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 15), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 16), "", 25.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 17), "", 34.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 18), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 19), "", 22.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 20), "", 25.4)
    );
    List<HeatWave> actualResult = HeatWave.calculateHeatWaves(inputList);

    List<HeatWave> expectedResult =
        List.of(new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 2), 7, 3, 31.8));

    // TODO: add assert
  }

  @Test
  void whenNoHeatWavesInInput_ThenEmptyList() {
    List<DailyTemperatureReading> inputList = List.of(
        new DailyTemperatureReading(LocalDate.of(2004, 6, 1), "", 21.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 2), "", 25.9),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 3), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 4), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 5), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 6), "", 22.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 7), "", 31.8),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 8), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 9), "", 22.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 10), "", 25.4)
    );
    List<HeatWave> actualResult = HeatWave.calculateHeatWaves(inputList);

    List<HeatWave> expectedResult = List.of();

    // TODO: add assert
  }

}
