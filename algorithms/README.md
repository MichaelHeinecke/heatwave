# Discussion Of Problem And Deduction Of Algorithms

## General Considerations

The problem that needs to be solved is calculating heat waves based on
the [KNMI definition](https://www.knmi.nl/kennis-en-datacentrum/uitleg/hittegolf):
> In the Netherlands, a heat wave is a succession of at least 5 summer days (maximum temperature 25.0 °C or higher) in
> De Bilt, of which at least three are tropical days (maximum temperature 30.0 °C or higher).

As there's a follow-up requirement to extend the application to calculate cold waves
their [KNMI definition](https://www.knmi.nl/kennis-en-datacentrum/uitleg/koudegolf) in mind:
> According to the KNMI definition, a cold wave is a continuous period in De Bilt of at least 5 ice days (maximum
> temperature lower than 0.0 degrees). Of these, the minimum temperature is lower than minus 10.0 degrees (severe frost)
> on at least 3 days.

For the discussion, we start out with developing a heat wave-specific algorithm and see if it can be generalized.

## Input Data

The input data are batches of a timeseries in which each record has temperature reading fields. Conceptually, the
relevant input data has the form as depicted in table below.

| Date       | Temperature |
|------------|-------------|
| 2004-06-01 | 21          |
| 2004-06-02 | 25          |
| 2004-06-03 | 22          |
| 2004-06-04 | 25          |
| 2004-06-05 | 26          |
| 2004-06-06 | 30          |
| 2004-06-07 | 30          |
| 2004-06-08 | 25          |
| 2004-06-09 | 31          |
| 2004-06-10 | 25          |
| 2004-06-11 | 22          |
| 2004-06-12 | 21          |
| 2004-06-13 | 26          |

In this timeseries, we need to identify:

1. a sequence of at least 5 consecutive days[^consecutive_days] representing consecutive dates with a maximum
   temperature of at least 25 degree, and
2. within this sequence, the presence of at least 3 records with a temperature of at least 30 degree.

[^consecutive_days]: Point 1 also implies that the data need to be checked for days not represented in the data.

## Deduction Of Algorithm

An expected example output of the algorithm is shown below.

| From date   | To date (inc.) | Duration (in days) | Number of tropical days | Max temperature |
|-------------|----------------|--------------------|-------------------------|-----------------|
| 31 jul 2003 | 13 aug 2003    | 14                 | 7                       | 35.0            |

Based on that, the algorithm needs to keep track of the following fields:

1. Start date
2. End date
3. Number of days
4. Number of tropical days (days with a maximum temperatures of at least 30 degrees)
5. Maximum temperature in the sequence

### An Array-Based Algorithm

Let the input be an array of the dates and an array of the temperatures in which `dates[i]` corresponds
to `temperatures[i]`. By keeping track of the aforementioned fields,

https://github.com/MichaelHeinecke/heatwave/blob/c1ce5c3e61db1b2dd1a488bc61bbc4073306a1af/algorithms/src/algorithms/array_based_algorithm.py#L13-L17

heat waves in the input data can be identified in a single pass over the temperatures array with linear time complexity
and linear space complexity (depending on the input array length).

https://github.com/MichaelHeinecke/heatwave/blob/acb4d0fdb5b4cb1f4f961625fd9da67585c23e97/algorithms/src/algorithms/array_based_algorithm.py#L43-L79

#### Extensibility To Cold Waves

By adding two fields

1. Number of ice days (maximum temperatures less than 0 degrees)
2. Number of severe frost days (minimum temperature less than -10 degrees)

and adding equivalent logic to check for cold waves, the algorithm could be applied to calculate cold waves as well.

#### <a id="horizontal-scalability-array-based">Horizontal Scalability</a>

Scaling the algorithm horizontally is a little trickier. To process data in parallel on multiple machines, the input
data has to be split and sent to other machines. If the data is split arbitrarily, data for a single heatwave might be
sent to separate machines, in which case the heat wave cannot be calculated, would be calculated as two separate ones,
or would be calculated but not in its full length.

The data would have to be partitioned appropriately to ensure that heat wave data aren't cut in the middle, before the
data are sent to separate workers.

#### Issue With Processing New Batches Of Data

The data comes in monthly batches. A heat wave might start in month n and continue in month n + 1. Hence, processing new
batches of data comes with an issue similar to the one described in the section
<a href="#horizontal-scalability-array-based">Horizontal Scalability</a>.

Damn you, batch processing! :D

There are at least two possible solutions:

1. The brute force approach: Reprocess everything. Simple but not efficient.
2. Reprocess parts of the data of month n (possibly relevant records identified by their temperature) when processing
   data of month n + 1. The data of month n could be filtered appropriately when processing data for month n + 1, or the
   potentially relevant records could be written to a dedicated location when processing data for month n and read them
   from there when processing data for month n + 1.

### A Window Function-Based Algorithm

At a higher level of abstraction operating on a dataframe, window functions can be used to calculate heat waves.

Going back to the same sample data, we need to identify which where a potential heat wave starts and where it ends.

| Date       | Temperature |
|------------|-------------|
| 2004-06-01 | 21          |
| 2004-06-02 | 25          |
| 2004-06-03 | 22          |
| 2004-06-04 | 25          |
| 2004-06-05 | 26          |
| 2004-06-06 | 30          |
| 2004-06-07 | 30          |
| 2004-06-08 | 25          |
| 2004-06-09 | 31          |
| 2004-06-10 | 25          |
| 2004-06-11 | 22          |
| 2004-06-12 | 21          |
| 2004-06-13 | 26          |

This can be done easily in four steps:

<ol>
<li>
Remove all records with temperatures lower than 25 degrees.

| Date       | Temperature |
|------------|-------------|
| 2004-06-02 | 25          |
| 2004-06-04 | 25          |
| 2004-06-05 | 26          |
| 2004-06-06 | 30          |
| 2004-06-07 | 30          |
| 2004-06-08 | 25          |
| 2004-06-09 | 31          |
| 2004-06-10 | 25          |
| 2004-06-13 | 26          |

</li>
<li>
Calculate the date difference for each row and it's preceding record using across a global window. If the difference is higher than 1 day, the
record marks the start of an uninterrupted sequence of days.

| Date       | Temperature | Difference To Previous Row | Start Of Sequence |
|------------|-------------|----------------------------|-------------------|
| 2004-06-02 | 25          | null                       | null              |
| 2004-06-04 | 25          | 2                          | 2004-06-04        |
| 2004-06-05 | 26          | 1                          | null              |
| 2004-06-06 | 30          | 1                          | null              |
| 2004-06-07 | 30          | 1                          | null              |
| 2004-06-08 | 25          | 1                          | null              |
| 2004-06-09 | 31          | 1                          | null              |
| 2004-06-10 | 25          | 1                          | null              |
| 2004-06-13 | 26          | 1                          | null              |

</li>
<li>
In the next step, each record that doesn't mark the start of a sequence will be assigned the last non-null value across a global window.

| Date       | Temperature | Start Of Sequence |
|------------|-------------|-------------------|
| 2004-06-02 | 25          | null              |
| 2004-06-04 | 25          | 2004-06-04        |
| 2004-06-05 | 26          | 2004-06-04        |
| 2004-06-06 | 30          | 2004-06-04        |
| 2004-06-07 | 30          | 2004-06-04        |
| 2004-06-08 | 25          | 2004-06-04        |
| 2004-06-09 | 31          | 2004-06-04        |
| 2004-06-10 | 25          | 2004-06-04        |
| 2004-06-13 | 26          | 2004-06-04        |

</li>
<li>
Lastly, grouping by the Start Of Sequence column, we can:
<ul>
<li>count the rows to get the duration of the potential heat wave</li>
<li>sum the number of days with at least 30 degrees (using a helper column with a value of 1 for qualified days and null otherwise)</li>
<li>find the maximum temperature</li>
<li>get the end date by adding the duration to the start date</li>
</ul>

By filtering the resulting dataframe on the duration (>=5 days) and the number of days with at least 30 degrees (>=3
days), we identify heat waves.
</li>
</ol>

There are two caveats to consider:

<ol>
<li>
The algorithm cannot calculate the difference to the previous row for the very first uninterrupted sequence of days
with at least 25 degrees found in the data. With a little additional logic, this edge case can be handled.<br>
However, there's an edge case of this edge case. An identified heatwave might be partial if the first remaining record 
after filtering out days with less than 25 degrees falls on the first of the month. In this case, the heat wave might
extend into the previous month, for which we have no data. Partial heat waves won't be calculated in the
implementation, but ultimately it also could be done if required by users.
</li>
<li>
As mentioned in the description above, the window functions are applied across a global window. Which requires moving
all data to a single partition, not allowing for any parallelism. This caveat will further be discussed in the 
section <a href="#horizontal-scalability-window-functions-based">Horizontal Scalability”</a>.
</li>
</ol>

An example implementation using PySpark, including handling the edge cases described above:

https://github.com/MichaelHeinecke/heatwave/blob/72f2bd39908bbf7c46742c2b2bdf06944f82d1c5/algorithms/src/algorithms/window_functions_based_algorithm.py#L36-L104

#### Extensibility To Cold Waves

The algorithm can be adapted for cold waves by:

1. Changing the filter expressions to remove any days with at least 0 degrees in step 1 of the algorithm.
2. Counting the number of days with temperatures less than -10 degrees in step 3.

#### <a id="horizontal-scalability-window-functions-based">Horizontal Scalability</a>

Similar considerations as for the array-based algorithm apply.

As mentioned as a caveat in the algorithm description, a global window across the entire dataframe is used. This means,
that all data is moved to a single partition, equivalent to be processed on a single worker. At the point during
processing the data where the algorithm is applied, the data needs to be sufficiently small.

Let's think this through for our actual input data, if it could become problematic:
The input data includes a record for every 10 minutes for multiple weather stations in the Netherlands with various data
points. For the algorithm, we only care about:

1. The data for a single weather station (De Bilt).
2. Only the datetime and temperature columns
3. The maximum temperature per day

Prior to the algorithm being applied, the pipeline will follow the steps:

1. Reading the data can happen in parallel, given that we have multiple input files in a non-splittable format.
2. Filtering for the relevant data can happen in a distributed fashion.
3. The temperature aggregation per day can also happen in a distributed fashion. As the aggregation requires a shuffle,
   to move records with the same key (the date) to the same partition which requires sending data between workers across
   the network. To make sure as little data as possible needs to be sent between workers, we need to ensure that
   filtering in step 2 happens prior to step 3 rather than after.

At this point, we will have one record per day for each day with at least 25 degrees. For 10 years worth of data, this
would be upper-capped at 3652 records. With this amount of data, we could even check the data manually for heat waves if
we really wanted. Meaning to say, packing this amount of data in a single partition and applying a global window to it
is nowhere near being problematic for performance.

#### Processing New Batches Of Data

Similar considerations as for the array-based algorithm apply.

If we want to avoid reprocessing the entire data with every new batch, we can process batches in a rolling fashion:

1. month n, month n + 1
2. month n + 1, month + 2
3. etc.

We don't want to calculate a potential partial heat wave at the beginning of the dataset being processed. As the
algorithm already doesn't identify the start date of sequence of days for the first sequence in the dataset being
processed, we can simply filter those records out.

What we do want, recalculating a heat wave that might extend into the next month. Assuming we have a heat wave that
starts in month n + 1 and carries on in month n + 2. When processing the second rolling batch, this heat wave will be
recalculated if there are additional eligible days at the beginning of month n + 2. A simple way of updating this heat
wave in our overall result dataset would be updating the pertaining record identified by using the start date of heat
waves as their key (implying that the data sink for the pipeline should support update operations).

If we really want to save on compute, the rolling batches could also be made conditional. Only read and process the
previous month's data along with the current month's data, if the last identified heat wave lasted until the last day of
the previous month and the current months data has an eligible day with at least 25 degrees on the first day of the
month.

### Streaming Algorithms

In the interest of time and as stream processing isn't warranted by the input data arriving in monthly batches, the
possible options are only briefly touched on without developing an algorithm.

#### Complex Event Processing

The basic concept of Complex Event Processing (CEP) is that pre-defined patterns are searched in a stream of events as
they arrive.

```mermaid
flowchart TB
   Pattern --> Stream
   subgraph Pattern
      sequence["🌞🔥🌞🔥🌞"]
   end
   subgraph Stream
      events["🥶🥶🌤️🌤️🌤️🌞🌞🔥🔥🌞🔥🌞🔥🌞🌤️🌤️🌤️🌤️🌤️🥶🥶🥶"]
   end
```

This approach could be used to match heat/cold wave patterns on a stream of temperature reading events. An
implementation could be done with the
library [FlinkCEP](https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/)

#### Stream Processing

It should be possible to create a stateful streaming application using the general idea of
the [array-based algorithm](#an-array-based-algorithm) with one of the many stream processing libraries.
