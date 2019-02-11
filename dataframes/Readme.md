DataFrames
====

This is a Java library to manage time-series and data-frames, in part inspired to the Python library [pandas].

The library is released under the [MIT License](https://opensource.org/licenses/MIT)

<https://github.com/sdu-cfei/java-libraries/dataframes/>

Two main classes are defined, `TimeSeries` and `DataFrames`.

A time-series represent a sequence of pairs (instant, value).
The list of instants is also called *index*.
Each time-series has a reading type, which is a subclass of `Number`.
Time-series can, therefore, contain values such as `Double`, `Integer` and `Long`.

A data-frame is a set of time-series having the same index, identified by a `Measure`.
The time-series contained in a data-frame can have different reading types.


Installation
----

This library uses Maven and can be installed with the following command.

~~~~bash
mvn install
~~~~


Documentation
----

[Javadoc](https://sdu-cfei.github.io/java-libraries/dataframes-docs/index.html)



Examples
----

In the following example, a time-series is created from a list of instants and a list of values.

~~~~java
Instant[] timestampsArray = {
    LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
    LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
    LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
    LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
    LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
Double[] valuesArray = {24.6, 28.7, 26.3, 13.8, 27.4};
ArrayList<Instant> timestamps = new ArrayList<>(
    Arrays.asList(timestampsArray));
ArrayList<Double> values = new ArrayList<>(
    Arrays.asList(valuesArray));

TimeSeries<Double> timeSeries = new TimeSeries<>(
    timestamps, values, Double.class);
~~~~

In the following example, a CSV file is parsed to generate a data-frame, some elements are modified, and a new CSV file is saved.

~~~~java
DataFrame dataFrame = DataFrame.fromCsv(new StringReader(csv),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        ZoneOffset.UTC);

TimeSeries<Double> timeSeries = ...
Measure measure = new Measure("fieldName", Double.class);
dataFrame.set(measure, timeSeries, false);

String modified = dataFrame.toCsv();
~~~~




[pandas]: https://pandas.pydata.org/
