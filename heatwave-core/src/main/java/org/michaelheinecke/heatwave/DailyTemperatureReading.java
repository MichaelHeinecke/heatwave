package org.michaelheinecke.heatwave;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * This record represents a row of data to be processed by heatwave algorithm.
 *
 * @param date           The date of the temperature reading.
 * @param location       The identifier of the weather station.
 * @param maxTemperature The maximum temperature.
 */
record DailyTemperatureReading(LocalDate date, String location, Double maxTemperature)
    implements Serializable {
  @Override
  public String toString() {
    return String.format("(date=%s, location=%s, maxTemperature=%.1f)", date.toString(), location,
        maxTemperature);
  }
}
