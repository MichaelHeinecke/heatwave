package org.michaelheinecke.heatwave;


import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * HeatWaveApp is home to the core algorithm of heatwave.
 *
 * <p>The algorithm calculates heat waves in the Netherlands following the definition of the KNMI:
 *
 * <p>A heat wave is a succession of at least 5 summer days (maximum temperature
 * > 25.0 °C or higher) in De Bilt, of which at least three are tropical days
 * > (maximum temperature 30.0 °C or higher).
 *
 * <p>The main method of this class is the entry point for running heatwave.
 */
public class HeatWaveApp {

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
      if (day.maxTemperature >= 25.0) {
        wave.numberOfDays++;
        if (wave.startDate == null) {
          wave.startDate = day.date;
        }
      }

      if (day.maxTemperature >= 30.0) {
        wave.numberOfTropicalDays++;
      }

      if (day.maxTemperature > wave.maxTemp) {
        wave.maxTemp = day.maxTemperature;
      }

      // If we reach the end of the input array,
      // or the end of an uninterrupted sequence of days with maximum temperatures of at least 25,
      // check if we found a heat wave.
      if (i == days.size() - 1 || ChronoUnit.DAYS.between(day.date, days.get(i + 1).date) > 1) {
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

  static LocalDate parseDateTime(String string) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDate.parse(string, formatter);
  }

  static void run(JavaSparkContext sc) {
    JavaRDD<DailyTemperatureReading> preprocessedRdd =
        sc.textFile("./data/kis_tot_20030*", 8).filter(line -> !line.startsWith("#"))
            .map(DailyTemperatureReading::parseRow)
            // Only keeps rows for weather station De Bilt.
            .filter(row -> Objects.equals(row.location, "260_T_a"))
            // Remove rows without temperature reading and are not potentially part of a heat wave.
            .filter(row -> row.maxTemperature != null && row.maxTemperature >= 25.0)
            // Find max temperature per date.
            .mapToPair(row -> (new Tuple2<>(row.date, row)))
            .reduceByKey((v1, v2) -> (v1.maxTemperature >= v2.maxTemperature ? v1 : v2))
            // Sort by date for the heatwave calculation algorithm.
            .sortByKey().values();

    List<DailyTemperatureReading> potentialHeatWaveDays = preprocessedRdd.collect();
    List<HeatWave> heatWaves = calculateHeatWaves(potentialHeatWaveDays);

    System.out.println(Arrays.toString(heatWaves.toArray()));
  }

  /**
   * This is the entry point for running heatwave.
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("HeatWaveApp").setMaster("local[*]");

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      run(sc);
    }

  }

  static class HeatWave {
    LocalDate startDate;
    LocalDate endDate;
    int startIndex;
    int endIndex;
    int numberOfDays;
    int numberOfTropicalDays;
    double maxTemp;

    HeatWave() {
      this.startDate = null;
      this.endDate = null;
      this.startIndex = 0;
      this.endIndex = 0;
      this.numberOfDays = 0;
      this.numberOfTropicalDays = 0;
      this.maxTemp = 0.0;
    }

    boolean isHeatwave() {
      return this.numberOfDays >= 5 && this.numberOfTropicalDays >= 3;
    }

    @Override
    public String toString() {
      return String.format(
          "From date: %s, to date: %s, duration: %d, Number Tropical Days: %d, Max Temperature: %"
              + ".1f", startDate, endDate, numberOfDays, numberOfTropicalDays, maxTemp);
    }
  }

  static class DailyTemperatureReading implements Serializable {
    LocalDate date;
    String location;
    Double maxTemperature;

    DailyTemperatureReading(LocalDate date, String location, Double maxTemperature) {
      this.date = date;
      this.location = location;
      this.maxTemperature = maxTemperature;
    }

    static DailyTemperatureReading parseRow(String row) {
      return new DailyTemperatureReading(parseDateTime(row.substring(0, 21).trim()),
          row.substring(21, 41).trim(), row.substring(309, 329).trim().equals("") ? null :
          Double.parseDouble(row.substring(309, 329).trim()));
    }

    @Override
    public String toString() {
      return String.format("(date=%s, location=%s, maxTemperature=%.1f)", date.toString(), location,
          maxTemperature);
    }
  }
}