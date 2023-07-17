package org.michaelheinecke.heatwave;

import java.io.Serializable;
import java.time.LocalDate;

class DailyTemperatureReading implements Serializable {
  LocalDate date;
  String location;
  Double maxTemperature;

  DailyTemperatureReading(LocalDate date, String location, Double maxTemperature) {
    this.date = date;
    this.location = location;
    this.maxTemperature = maxTemperature;
  }

  static DailyTemperatureReading parseRow(String row) {
    return new DailyTemperatureReading(HeatWaveApp.parseDateTime(row.substring(0, 21).trim()),
        row.substring(21, 41).trim(), row.substring(309, 329).trim().equals("") ? null :
        Double.parseDouble(row.substring(309, 329).trim()));
  }

  @Override
  public String toString() {
    return String.format("(date=%s, location=%s, maxTemperature=%.1f)", date.toString(), location,
        maxTemperature);
  }
}
