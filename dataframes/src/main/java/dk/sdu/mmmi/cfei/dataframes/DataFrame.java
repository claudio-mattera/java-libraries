package dk.sdu.mmmi.cfei.dataframes;

import java.io.IOException;
import java.io.Reader;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * Represents a table of readings for a series of date times.
 *
 * <pre>
 * {@code
 * Time             | Temperature [C] | Humidity [%]
 * -----------------+-----------------+--------------
 * 2015-01-05 12:55 |            24.6 |           34
 * 2015-01-05 12:59 |            28.7 |           24
 * 2015-01-05 15:08 |            26.3 |           64
 * }
 * </pre>
 *
 * @author cgim
 */
public class DataFrame implements Iterable<MultipleReading> {

    /**
     * Create an empty data frame.
     *
     * @param datetimes The date times.
     */
    public DataFrame(List<Instant> datetimes) {
        this.datetimes = new ArrayList<>(datetimes);
        this.columns = new ArrayList<>();
        this.data = new HashMap<>();
    }

    /**
     * Return the column of the data frame.
     *
     * @return A list of columns.
     */
    public List<Measure> getColumns() {
        return new ArrayList<>(this.columns);
    }

    /**
     * Return the values for a given column.
     *
     * @param measure The column.
     * @return A time series..
     */
    public TimeSeries<Number> getColumn(Measure measure) {
        return new TimeSeries(datetimes, this.data.get(measure), measure.type);
    }

    /**
     * Set the values for a column from a given time series.
     *
     * Note that the other time series must be contained in this time series.
     *
     * @param <T> The value type.
     * @param measure The column.
     * @param that The other time series.
     * @param skipNaNs Skip NaN values.
     */
    public <T extends Number> void set(Measure measure, TimeSeries<T> that, boolean skipNaNs) {
        List<Number> values = this.data.get(measure);
        for (int i = 0; i < that.size(); ++i) {
            int j = this.datetimes.indexOf(that.getReading(i).getDatetime());
            T value = that.getReading(i).getValue();
            if (skipNaNs && value instanceof Double && ((Double) value).isNaN()) {
                // Skip
            } else {
                values.set(j, value);
            }
        }
    }

    /**
     * Set the values for a column from a given contiguous time series.
     *
     * Note that the other time series must be contained in this time series and
     * that the other time series must not have any hole. If there are holes,
     * use set(Measure, TimeSeries).
     *
     * @param <T> The value type.
     * @param measure The column.
     * @param that The other time series.
     */
    public <T extends Number> void setContiguous(Measure measure, TimeSeries<T> that) {
        List<Number> values = this.data.get(measure);
        final int offset = this.datetimes.indexOf(that.getReading(0).getDatetime());
        for (int i = 0; i < that.size(); ++i) {
            assert this.datetimes.get(i + offset).equals(that.getReading(i).getDatetime());
            values.set(i + offset, that.getReading(i).getValue());
        }
    }

    /**
     * Return the size of the data frame.
     *
     * @return The size of the data frame.
     */
    public int size() {
        return this.datetimes.size();
    }

    /**
     * Return the values for a given row.
     *
     * @param i The row index.
     * @return A multiple reading.
     */
    public MultipleReading getRow(int i) {
        List values = columns.stream()
                .map(col -> data.get(col).get(i))
                .collect(Collectors.toList());
        return new MultipleReading(datetimes.get(i), values);
    }

    /**
     * Return the values for the last row.
     *
     * @return A multiple reading.
     */
    public MultipleReading getLastRow() {
        final int index = this.size() - 1;
        List values = columns.stream()
                .map(col -> data.get(col).get(index))
                .collect(Collectors.toList());
        return new MultipleReading(datetimes.get(index), values);
    }

    /**
     * Add a new column.
     *
     * @param measure The new column.
     * @param column The new column values.
     */
    public void addColumn(Measure measure, List column) {
        if (!this.columns.contains(measure)) {
            this.columns.add(measure);
        }
        this.data.put(measure, new ArrayList<>(column));
    }

    /**
     * Resample a data frame.
     *
     * @param duration The new step.
     * @return A data frame obtained by interpolating the original data frame
     * with a new step.
     */
    public DataFrame resample(Duration duration) {
        Instant startTime = datetimes.get(0);
        Instant stopTime = datetimes.get(datetimes.size() - 1);
        return resample(startTime, stopTime, duration);
    }

    /**
     * Resample a data frame.
     *
     * @param step The new step in seconds.
     * @return A data frame obtained by interpolating the original data frame
     * with a new starting/ending time and a new step.
     */
    public DataFrame resample(long step) {
        return resample(Duration.ofSeconds(step));
    }

    /**
     * Append a data frame.
     *
     * Note: No checking on the index is performed, make sure that the index
     * stays monotonous increasing.
     *
     * @param that The time series to append
     * @throws RuntimeException if the two data frames have different columns
     */
    public void append(DataFrame that) {
        if (!this.columns.equals(that.columns)) {
            throw new RuntimeException("Data frames have different columns");
        }
        this.datetimes.addAll(that.datetimes);
        this.data.forEach((measure, list) -> {
            list.addAll(that.data.get(measure));
        });
    }

    /**
     * Resample a data frame.
     *
     * @param startTime New starting time (must be within the original data
     * frame).
     * @param stopTime New ending time (must be within the original data frame).
     * @param step The new step.
     * @return A data frame obtained by interpolating the original data frame
     * with a new starting/ending time and a new step.
     */
    public DataFrame resample(Instant startTime, Instant stopTime, Duration step) {
        // Get beginTime and endTime
        Instant beginTimeOld = datetimes.get(0);
        Instant endTimeOld = datetimes.get(datetimes.size() - 1);

        // Create new time array
        ArrayList<Instant> newIndex = getNewTimeline(startTime, stopTime, step);
        ArrayList<ArrayList<Double>> newValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            newValues.add(new ArrayList<>());
        }

        if (startTime.isBefore(beginTimeOld) || stopTime.isAfter(endTimeOld)) {
            throw new IndexOutOfBoundsException(
                    MessageFormat.format(
                            "Cannot resample from ({0} -> {1}) to ({2} -> {3})",
                            beginTimeOld,
                            endTimeOld,
                            startTime,
                            stopTime
                    )
            );
        }

        // Interpolate data
        int nStepOld = 0;
        for (Instant t : newIndex) {

            if ((t.isAfter(beginTimeOld) || t.equals(beginTimeOld)) && (t.isBefore(endTimeOld) || t.equals(endTimeOld))
                    && (t.isAfter(startTime) || t.equals(startTime)) && (t.isBefore(stopTime) || t.equals(stopTime))) {

                while (t.isAfter(datetimes.get(nStepOld))) {
                    if ((nStepOld + 1) < datetimes.size()) {
                        nStepOld++;
                    } else {
                        break; // timeOld[nStepOld] out of range
                    }
                }

                if (nStepOld == 0) {
                    // You are between timeOld[0] and timeOld[1]
                    // Forward interpolation:
                    final long tPrev = datetimes.get(nStepOld).toEpochMilli();
                    final long tNext = datetimes.get(nStepOld + 1).toEpochMilli();

                    for (int i = 0; i < columns.size(); ++i) {
                        double yPrev
                                = ((Number) data.get(columns.get(i))
                                        .get(nStepOld))
                                        .doubleValue();
                        double yNext
                                = ((Number) data.get(columns.get(i))
                                        .get(nStepOld + 1))
                                        .doubleValue();
                        // t - current time (from timeNew)

                        double y = (((yNext - yPrev) / (tNext - tPrev))
                                * (t.toEpochMilli() - tPrev)) + yPrev;

                        newValues.get(i).add(y);
                    }

                } else if (nStepOld > 0) {
                    // You are between timeOld[nStepOld-1] and timeOld[nStepOld]
                    // Backward interpolation:
                    long tPrev = datetimes.get(nStepOld - 1).toEpochMilli();
                    long tNext = datetimes.get(nStepOld).toEpochMilli();

                    for (int i = 0; i < columns.size(); ++i) {
                        double yPrev
                                = ((Number) data.get(columns.get(i))
                                        .get(nStepOld - 1))
                                        .doubleValue();
                        double yNext
                                = ((Number) data.get(columns.get(i))
                                        .get(nStepOld))
                                        .doubleValue();
                        // t - current time (from timeNew)

                        double y = (((yNext - yPrev) / (tNext - tPrev))
                                * (t.toEpochMilli() - tPrev)) + yPrev;

                        newValues.get(i).add(y);
                    }
                }
            }
        }

        DataFrame df = new DataFrame(newIndex);
        for (int i = 0; i < columns.size(); ++i) {
            assert newValues.get(i).size() == newIndex.size();
            df.addColumn(columns.get(i), newValues.get(i));
        }

        return df;
    }

    public static DataFrame fromCsv(
            Reader reader,
            DateTimeFormatter formatter,
            ZoneId timezone
    ) throws IOException {
        return fromCsv(reader, formatter, timezone, new HashMap<>(), new HashMap<>());
    }

    public static DataFrame fromCsv(
            Reader reader,
            DateTimeFormatter formatter,
            ZoneId timezone,
            List<Class> dtypes
    ) throws IOException {
        Map<Integer, Function<String, Number>> parsers = IntStream
                .range(0, dtypes.size())
                .mapToObj(i -> i)
                .collect(Collectors.toMap(
                        i -> i,
                        i -> dtypes.get(i).equals(Integer.class) ? parseInteger : parseDouble
                ));

        Map<Integer, Class> dtypesMap = IntStream
                .range(0, dtypes.size())
                .mapToObj(i -> i)
                .collect(Collectors.toMap(
                        i -> i,
                        i -> dtypes.get(i)
                ));

        return fromCsv(reader, formatter, timezone, parsers, dtypesMap);
    }

    private static DataFrame fromCsv(
            Reader reader,
            DateTimeFormatter formatter,
            ZoneId timezone,
            Map<Integer, Function<String, Number>> parsers,
            Map<Integer, Class> dtypes
    ) throws IOException {
        List<Instant> index = new ArrayList<>();
        Map<Integer, List<Number>> values = new HashMap<>();
        Map<Integer, String> headers = new HashMap<>();
        CSVParser parser = CSVFormat.DEFAULT.withHeader().withIgnoreSurroundingSpaces().parse(reader);
        parser.getHeaderMap().entrySet().stream().forEach(entry -> {
            headers.put(entry.getValue(), entry.getKey());
            values.put(entry.getValue(), new ArrayList<>());
        });
        for (CSVRecord record : parser) {
            String datetimeString = record.get(0);
            LocalDateTime datetime = LocalDateTime.parse(datetimeString, formatter);
            index.add(datetime.atZone(timezone).toInstant());
            for (int i = 1; i < record.size(); ++i) {
                String string = record.get(i);
                values.get(i).add(parsers.getOrDefault(i - 1, parseDouble).apply(string));
            }
        }

        DataFrame dataframe = new DataFrame(index);
        values.entrySet().stream().filter(entry -> entry.getKey() > 0).forEach(entry -> {
            final Class dtype = dtypes.getOrDefault(entry.getKey() - 1, Double.class);
            Measure measure = new Measure(headers.get(entry.getKey()), dtype);
            dataframe.addColumn(measure, values.get(entry.getKey()));
        });

        return dataframe;
    }

    public String toCsv() {
        StringBuilder builder = new StringBuilder();
        try {
            CSVPrinter printer = new CSVPrinter(builder, CSVFormat.DEFAULT);
            printer.print("datetime");
            for (Measure column : this.getColumns()) {
                printer.print(column.name);
            }
            printer.println();
            for (MultipleReading reading : this) {
                printer.print(reading.getDatetime());
                for (Object value : reading) {
                    printer.print(escapeNan(value));
                }
                printer.println();
            }
        } catch (IOException ex) {
            // Can't happen when writing to StringBuilder.
        }
        return builder.toString()
                .trim() // Remove final newline
                .replace("\r\n", "\n");
    }

    public void toCsv(Appendable out) throws IOException {
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
        printer.print("datetime");
        for (Measure column : this.getColumns()) {
            printer.print(column.name);
        }
        printer.println();
        for (MultipleReading reading : this) {
            printer.print(reading.getDatetime());
            for (Object value : reading) {
                printer.print(escapeNan(value));
            }
            printer.println();
        }
    }

    private ArrayList<Instant> getNewTimeline(
            Instant startTime, Instant stopTime, Duration step) {
        ArrayList<Instant> timeNew = new ArrayList<>();
        Instant t = startTime;
        while (t.isBefore(stopTime) || t.equals(stopTime)) {
            timeNew.add(t);
            t = t.plus(step);
        }
        return timeNew;
    }

    /**
     * Access the data frame as a stream of readings.
     *
     * @return A stream of readings.
     */
    public Stream<MultipleReading> stream() {
        return StreamSupport.stream(this.spliterator(), true);
    }

    @Override
    public Iterator<MultipleReading> iterator() {

        return new Iterator<MultipleReading>() {

            @Override
            public boolean hasNext() {
                return datetimesIterator.hasNext();
            }

            @Override
            public MultipleReading next() {
                List values = valuesIterators.stream()
                        .map(iterator -> iterator.next())
                        .collect(Collectors.toList());
                MultipleReading reading
                        = new MultipleReading(datetimesIterator.next(), values);
                return reading;
            }

            private final List<Iterator> valuesIterators = columns.stream()
                    .map(col -> data.get(col).iterator())
                    .collect(Collectors.toList());
            private final Iterator<Instant> datetimesIterator = datetimes.iterator();
        };
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.datetimes);
        hash = 53 * hash + Objects.hashCode(this.data);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DataFrame other = (DataFrame) obj;
        if (!Objects.equals(this.datetimes, other.datetimes)) {
            return false;
        }
        return Objects.equals(this.data, other.data);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Time, ");
        builder.append(
                this.columns.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(", ")));
        builder.append("\n");

        for (MultipleReading reading : this) {
            String datetimeString = reading.getDatetime().toString();
            builder.append(datetimeString);
            builder.append(", ");

            for (Object value : reading) {
                builder.append(String.valueOf(value));
                builder.append(", ");
            }

            builder.append("\n");
        }

        return "DataFrame{" + builder.toString() + '}';
    }

    private Object escapeNan(Object value) {
        if (value instanceof Double) {
            if (((Double) value).isNaN()) {
                return "";
            }
        }

        return value;
    }

    private final List<Instant> datetimes;
    private final List<Measure> columns;
    private final Map<Measure, List> data;

    private static final Function<String, Number> parseDouble = string -> string.isEmpty() ? Double.NaN : Double.valueOf(string);
    private static final Function<String, Number> parseInteger = string -> {
        try {
            return Integer.valueOf(string);
        } catch (NumberFormatException e) {
            return Double.valueOf(string).intValue();
        }
    };
}
