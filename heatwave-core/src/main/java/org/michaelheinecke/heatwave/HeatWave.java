package org.michaelheinecke.heatwave;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * HeatWave is home to the core algorithm of heatwave.
 *
 * <p>The algorithm calculates heat waves in the Netherlands following the definition of the KNMI:
 *
 * <p>A heat wave is a succession of at least 5 summer days (maximum temperature
 * > 25.0 °C or higher) in De Bilt, of which at least three are tropical days
 * > (maximum temperature 30.0 °C or higher).
 *
 * <p>HeatWave objects represent potential heat waves.
 */
class HeatWave {
  LocalDate startDate;
  LocalDate endDate;
  int numberOfDays;
  int numberOfTropicalDays;
  double maxTemp;

  HeatWave() {
    this.startDate = null;
    this.endDate = null;
    this.numberOfDays = 0;
    this.numberOfTropicalDays = 0;
    this.maxTemp = 0.0;
  }

  /**
   * Check if HeatWave instance is a heat wave following the KNMI definition of minimum 5 days
   * and minimum 3 tropical days.
   *
   * @return True if HeatWave is a heat wave, otherwise false.
   */
  boolean isHeatwave() {
    return this.numberOfDays >= 5 && this.numberOfTropicalDays >= 3;
  }

  @Override
  public String toString() {
    return String.format(
        "From date: %s, to date: %s, duration: %d, Number Tropical Days: %d, Max Temperature: %"
            + ".1f", startDate, endDate, numberOfDays, numberOfTropicalDays, maxTemp);
  }

  /**
   * Core algorithm to calculate heat waves.
   *
   * @param days A list of DailyTemperatureReading objects, sorted by date in ascending order,
   *             and filtered for days with a maximum temperature of at least 25.0 °C.
   * @return A list of HeatWave objects, representing a heat wave.
   */
  static List<HeatWave> calculateHeatWaves(List<DailyTemperatureReading> days) {
    List<HeatWave> heatwaves = new ArrayList<>();
    HeatWave wave = new HeatWave();

    for (int i = 0; i < days.size(); i++) {
      DailyTemperatureReading day = days.get(i);
      wave.numberOfDays++;
      if (wave.startDate == null) {
        wave.startDate = day.date();
      }

      if (day.maxTemperature() >= 30.0) {
        wave.numberOfTropicalDays++;
      }

      if (day.maxTemperature() > wave.maxTemp) {
        wave.maxTemp = day.maxTemperature();
      }

      // If we reach the end of the input array,
      // or the end of an uninterrupted sequence of days with maximum temperatures of at least 25,
      // check if we found a heat wave.
      if (i == days.size() - 1 || ChronoUnit.DAYS.between(day.date(), days.get(i + 1).date()) > 1) {
        if (wave.isHeatwave()) {
          // Calculate the inclusive end date of the wave.
          wave.endDate = wave.startDate.plusDays(wave.numberOfDays - 1);
          heatwaves.add(wave);
        }
        // Reset fields to check next potential heatwave.
        wave = new HeatWave();
      }
    }

    return heatwaves;
  }
}
