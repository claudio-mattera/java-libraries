DataFrames
====

This is a Java library to read and write [EnergyPlus] weather (EPW) files.

<https://github.com/sdu-cfei/java-libraries/epwformat/>


Installation
----

This library uses Maven and can be installed with the following command.

~~~~bash
mvn install
~~~~

EPW files are CSV files with an additional header, therefore, this library used the [DataFrames](https://github.com/sdu-cfei/java-libraries/dataframes/) library for managing the raw data.


Documentation
----

[Javadoc](https://sdu-cfei.github.io/java-libraries/epwformat-docs/index.html)



Examples
----

In the following example, the `dryBulbTemp` column of the EPW file is partially replaced with new readings.

~~~~java
String rawEpwContent = ...

EpwParser parser = new EpwParser(year, timeZone);
Epw weather = parser.parse(rawEpwContent);

String field = "dryBulbTemp";
TimeSeries<Double> timeSeries = ...; // Must be a proper subset of the original

weather.modifyDataframe(dataFrame -> {
    if (Epw.getDatatypeForField(field).equals(Integer.class)) {
        // Casting time-series to integer
        TimeSeries<Integer> integerTimeSeries = timeSeries.toInteger();
        Measure measure = new Measure(field, Integer.class);
        dataFrame.setContiguous(measure, integerTimeSeries);
    } else {
        final Measure measure = new Measure(field, Double.class);
        dataFrame.setContiguous(measure, timeSeries);
    }
});

String newRawEpwContent = weather.export();
~~~~



[EnergyPlus]: https://energyplus.net/
