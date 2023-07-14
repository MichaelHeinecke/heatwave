# heatwave

Home to the code for the Xebia Data assignment.

# Discussion Of Problem And Deduction Of Algorithm

## General Considerations

The problem that needs to be solved is calculating heat waves based on
the [KNMI definition](https://www.knmi.nl/kennis-en-datacentrum/uitleg/hittegolf):
> In the Netherlands, a heat wave is a succession of at least 5 summer days (maximum temperature 25.0 °C or higher)  in
> De Bilt, of which at least three are tropical days (maximum temperature 30.0 °C or higher).

As there's a follow-up requirement to extend the application to calculate cold waves
their [KNMI definition](https://www.knmi.nl/kennis-en-datacentrum/uitleg/koudegolf) in mind:
> According to the KNMI definition, a cold wave is a continuous period in De Bilt of at least 5 ice days (maximum
> temperature lower than 0.0 degrees). Of these, the minimum temperature is lower than minus 10.0 degrees (severe frost)
> on at least 3 days.

For the discussion, we start out with developing a heat wave-specific algorithm and see if it can be generalized.

## Input Data

The input data are a timeseries in which each record has temperature reading fields. Conceptually, the relevant input
data has the form as depicted in table below.

| Date       | Temperature |
|------------|-------------|
| 2004-06-01 | 21          |
| 2004-06-02 | 24          |
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

In this timeseries, we need to identify:

1. a sequence of at least 5 consecutive days[^consecutive_days] representing consecutive dates with a maximum
   temperature of at least 25 degree, and
2. within this sequence, the presence of at least 3 records with a temperature of at least 30 degree.

[^consecutive_days]: Point 1 also implies that the data need to be checked for days not represented in the data.
