package org.michaelheinecke.heatwave;


import static org.michaelheinecke.heatwave.HeatWave.calculateHeatWaves;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * HeatWaveApp is the entry point for running heatwave.
 *
 * <p>HeatWaveApp reads and pre-processes the data, and writes the results to a file.
 *
 * <p>When running the app, a path to the input data files needs to be passed in.
 */
public class HeatWaveApp {
  private static final Logger LOG = LoggerFactory.getLogger(HeatWaveApp.class);
  private static final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  /**
   * The run method reads the input files, pre-processes the data, and calls the algorithm to find
   * heat waves in the data.
   *
   * @param sc   A JavaSparkContext.
   * @param path A path to the input files to be processed.
   */
  static void run(JavaSparkContext sc, String path) {
    JavaRDD<DailyTemperatureReading> preprocessedRdd = sc.textFile(path, 8)
        // Remove header rows
        .filter(line -> !line.startsWith("#"))
        // Parse the relevant data from the input file lines.
        .map(row ->
            new DailyTemperatureReading(LocalDate.parse(row.substring(0, 21).strip(), formatter),
                row.substring(21, 41).strip(),
                row.substring(309, 329).strip().equals("") ? null :
                    Double.parseDouble(row.substring(309, 329).strip()))
        )
        // Only keeps rows for weather station De Bilt.
        .filter(row -> Objects.equals(row.location(), "260_T_a"))
        // Remove rows without temperature reading and are not potentially part of a heat wave.
        .filter(row -> row.maxTemperature() != null && row.maxTemperature() >= 25.0)
        // Find max temperature per date.
        .mapToPair(row -> (new Tuple2<>(row.date(), row)))
        .reduceByKey((v1, v2) -> (v1.maxTemperature() >= v2.maxTemperature() ? v1 : v2))
        // Sort by date for the heatwave calculation algorithm.
        .sortByKey().values();

    List<DailyTemperatureReading> potentialHeatWaveDays = preprocessedRdd.collect();
    List<HeatWave> heatWaves = calculateHeatWaves(potentialHeatWaveDays);

    LOG.info("Calculated result:\n{}", Arrays.toString(heatWaves.toArray()));

    String fileName = "heatwaves.txt";
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(fileName), StandardCharsets.UTF_8))) {
      for (HeatWave heatWave : heatWaves) {
        writer.write(String.valueOf(heatWave));
        writer.write(System.lineSeparator());
      }
    } catch (IOException e) {
      LOG.error("Encountered error while");
      throw new RuntimeException(e);
    }
    LOG.info("Wrote results to {}", fileName);
  }

  /**
   * This is the entry point for running heatwave.
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Please pass a path to the data to be processed. Exiting.");
      System.exit(1);
    }

    String path = args[0];
    LOG.info("Reading files from path: {}", path);

    SparkConf conf = new SparkConf().setAppName("HeatWaveApp").setMaster("local[*]");

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      run(sc, path);
    }

  }

}