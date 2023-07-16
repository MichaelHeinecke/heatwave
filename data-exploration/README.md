# Summary Of Data Exploration

The data-exploration notebook contains an analysis of the input files. A summary is below.

## File Structure

* Each file contains a multi-line header of differing length. The header lines start with "#" and an easily be removed.
* The files are in a fixed-width format. Columns can be separated by parsing substrings using the respective column
  widths. This may be brittle in a production scenario with new batches of data. If the column width ever changes,
  parsing will fail.

## Observations On Data Statistics

* As we can see from the monthly input files, we don't have data for all month in the years 2003 and 2020.
* There should be 52560 records (one for each 10 minutes; 60 * 24 * 365 / 10) in each regular year and 52704 records in
  a leap year. For most years, we have complete data. Exceptions are 2010 with 52416 records, and 2015 with 51264
  records. Given this, the data should be reasonably complete. As we will disregard missing data, there's no need to
  further worry about this.
* The window functions-based algorithm aggregates the records by day. As only max/min aggregations are used, we don't
  need to take care of duplicates.
* There are no unreasonable temperature outliers that would need further investigation.
* The minimum and maximum temperature columns' min and max values are close to the temperature columns min and max
  values across all years, giving no indication for a closer look.
* Compared to the temperature column (t_dryb_10), the minimum temperature(tn_dryb_10) and maximum temperature (
  tn_dryb_10) columns have a few more missing readings (less than a dozen per year). To fill in those blanks, we can
  consider coalescing min/max temperature columns and the temperature column.