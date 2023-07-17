package org.michaelheinecke.heatwave;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

public class HeatWaveTest {

  @Test
  void whenHeatWaveInInput_ThenWaveIsFound() {
    List<DailyTemperatureReading> inputList = List.of(
        new DailyTemperatureReading(LocalDate.of(2004, 6, 2), "", 25.9),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 3), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 4), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 5), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 6), "", 25.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 7), "", 31.8),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 8), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 10), "", 25.4)
    );
    List<HeatWave> actualResult = HeatWave.calculateHeatWaves(inputList);

    HeatWave expectedFirstHeatWave =
        new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 8), 7, 3, 31.8);

    assertThat(actualResult, hasSize(1));
    assertThat(actualResult, containsInAnyOrder(expectedFirstHeatWave));
  }

  @Test
  void whenMultiplesHeatWavesInInput_ThenAllFound() {
    List<DailyTemperatureReading> inputList = List.of(
        new DailyTemperatureReading(LocalDate.of(2004, 6, 2), "", 25.9),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 3), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 4), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 5), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 6), "", 25.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 7), "", 31.8),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 8), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 10), "", 25.4),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 13), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 14), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 15), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 16), "", 25.3),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 17), "", 34.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 18), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 20), "", 25.4)
    );
    List<HeatWave> actualResult = HeatWave.calculateHeatWaves(inputList);


    HeatWave expectedFirstHeatWave =
        new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 8), 7, 3, 31.8);
    HeatWave expectedSecondHeatWave =
        new HeatWave(LocalDate.of(2004, 6, 13), LocalDate.of(2004, 6, 18), 6, 3, 34.1);

    assertThat(actualResult, hasSize(2));
    assertThat(actualResult, containsInAnyOrder(expectedFirstHeatWave, expectedSecondHeatWave));
  }

  @Test
  void whenNoHeatWavesInInput_ThenEmptyList() {
    List<DailyTemperatureReading> inputList = List.of(
        new DailyTemperatureReading(LocalDate.of(2004, 6, 2), "", 25.9),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 3), "", 26.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 4), "", 30.6),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 5), "", 30.0),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 7), "", 31.8),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 8), "", 25.1),
        new DailyTemperatureReading(LocalDate.of(2004, 6, 10), "", 25.4)
    );
    List<HeatWave> actualResult = HeatWave.calculateHeatWaves(inputList);

    assertThat(actualResult, hasSize(0));
  }

  @Test
  void whenIsHeatWave_ThenHeatWave() {
    HeatWave heatWave = new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 6), 5, 3, 31.8);
    assertThat(heatWave.isHeatwave(), is(true));
  }

  @Test
  void whenNumberOfDaysTooLow_ThenNotHeatWave() {
    HeatWave heatWave = new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 5), 4, 3, 31.8);
    assertThat(heatWave.isHeatwave(), is(false));
  }

  @Test
  void whenNumberOfTropicalDaysTooLow_ThenNotHeatWave() {
    HeatWave heatWave = new HeatWave(LocalDate.of(2004, 6, 2), LocalDate.of(2004, 6, 8), 7, 2, 31.8);
    assertThat(heatWave.isHeatwave(), is(false));
  }

}
