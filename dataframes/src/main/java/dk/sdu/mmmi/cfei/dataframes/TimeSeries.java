package dk.sdu.mmmi.cfei.dataframes;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A time series of values.
 *
 * A time series is a sequence of pairs: a datetime and a value.
 *
 * @author cgim
 * @param <T> Type of values.
 */
public class TimeSeries<T extends Number> implements Iterable<Reading<T>> {

    /**
     * Create an empty time series.
     *
     * @param clazz Data type.
     */
    public TimeSeries(Class<T> clazz) {
        this(0, clazz);
    }

    /**
     * Create an empty time series.
     *
     * @param i Number of elements to preallocate.
     * @param clazz Data type.
     */
    public TimeSeries(int i, Class<T> clazz) {
        this.datetimes = new ArrayList<>(i);
        this.values = new ArrayList<>(i);
        this.clazz = clazz;
    }

    /**
     * Create a time series from a list of datetimes and a list of values.
     *
     * @param datetimes A list of datetimes.
     * @param values A list of values.
     * @param clazz Data type.
     */
    public TimeSeries(List<Instant> datetimes, List<T> values, Class<T> clazz) {
        this.datetimes = new ArrayList<>(datetimes);
        this.values = new ArrayList<>(values);
        this.clazz = clazz;
    }

    /**
     * Return the size of the time series.
     *
     * @return The size of the time series.
     */
    public int size() {
        return this.datetimes.size();
    }

    /**
     * Return the data type of the time series.
     *
     * @return The data type of the time series.
     */
    public Class getDataType() {
        return this.clazz;
    }

    /**
     * Return a reading at a given position.
     *
     * @param i The position.
     * @return A reading.
     */
    public Reading<T> getReading(int i) {
        return new Reading(this.datetimes.get(i), this.values.get(i), this.clazz);
    }

    /**
     * Return a reading at the last position.
     *
     * @return A reading.
     */
    public Reading<T> getLastReading() {
        final int index = this.size() - 1;
        return new Reading(this.datetimes.get(index), this.values.get(index), this.clazz);
    }

    /**
     * Return a reading at a given datetime.
     *
     * @param datetime The datetime.
     * @return A reading.
     */
    public Reading<T> get(Instant datetime) {
        final int index = this.datetimes.indexOf(datetime);
        return new Reading(this.datetimes.get(index), this.values.get(index), this.clazz);
    }

    /**
     * Append a reading to the time series.
     *
     * @param reading The reading to append.
     */
    public void addReading(Reading<T> reading) {
        this.datetimes.add(reading.getDatetime());
        this.values.add(reading.getValue());
    }

    /**
     * Insert a reading into the time series.
     *
     * @param i The index where to insert the specified element
     * @param reading The reading to insert.
     */
    public void addReading(int i, Reading<T> reading) {
        this.datetimes.add(i, reading.getDatetime());
        this.values.add(i, reading.getValue());
    }

    /**
     * Remove readings satisfying a predicate.
     *
     * @param predicate The predicate.
     */
    public void removeReadings(Predicate<Reading<T>> predicate) {
        Iterator<Instant> ii = this.datetimes.iterator();
        Iterator<T> iv = this.values.iterator();
        while (ii.hasNext()) {
            Reading reading = new Reading(ii.next(), iv.next(), this.clazz);
            if (predicate.test(reading)) {
                ii.remove();
                iv.remove();
            }
        }
    }

    /**
     * Append a time series.
     *
     * Note: No checking on the index is performed, make sure that the index
     * stays monotonous increasing.
     *
     * @param that The time series to append
     */
    public void append(TimeSeries<T> that) {
        this.datetimes.addAll(that.datetimes);
        this.values.addAll(that.values);
    }

    /**
     * Access the list of datetimes.
     *
     * @return A list of datetimes.
     */
    public List<Instant> getDatetimes() {
        return this.datetimes;
    }

    /**
     * Access the list of values.
     *
     * @return A list of values.
     */
    public List<T> getValues() {
        return this.values;
    }

    /**
     * Set the value at a given datetime.
     *
     * @param datetime The datetime.
     * @param value The new value.
     */
    public void set(Instant datetime, T value) {
        final int index = this.datetimes.indexOf(datetime);
        this.values.set(index, value);
    }

    /**
     * Set the values from a given time series.
     *
     * For instance, given a first time series:
     *
     * <pre>
     * {@code
     * first =
     *     2015-01-05 12:55 | 24.6
     *     2015-01-05 12:59 | 28.7
     *     2015-01-05 15:08 | 26.3
     *     2015-01-05 15:28 | 13.8
     *     2015-01-05 17:08 | 27.4
     * }
     * </pre>
     *
     * and a second time series:
     *
     * <pre>
     * {@code
     * second =
     *     2015-01-05 15:08 | 999.0
     *     2015-01-05 15:28 | 666.6
     * }
     * </pre>
     *
     * calling
     *
     * <pre>
     * {@code
     * first.set(second);
     * }
     * </pre>
     *
     * will set the values of first to
     *
     * <pre>
     * {@code
     * first =
     *     2015-01-05 12:55 | 24.6
     *     2015-01-05 12:59 | 28.7
     *     2015-01-05 15:08 | 999.0
     *     2015-01-05 15:28 | 666.6
     *     2015-01-05 17:08 | 27.4
     * }
     * </pre>
     *
     * Note that the other time series must be contained in this time series and
     * that the other time series must not have any hole. If there are holes,
     * use set(TimeSeries).
     *
     * @param that The other time series.
     */
    public void setContiguous(TimeSeries<T> that) {
        final int offset = this.datetimes.indexOf(that.datetimes.get(0));
        for (int i = 0; i < that.size(); ++i) {
            assert this.datetimes.get(i + offset).equals(that.datetimes.get(i));
            this.values.set(i + offset, that.values.get(i));
        }
    }

    /**
     * Set the values from a given time series.
     *
     * For instance, given a first time series:
     *
     * <pre>
     * {@code
     * first =
     *     2015-01-05 12:55 | 24.6
     *     2015-01-05 12:59 | 28.7
     *     2015-01-05 15:08 | 26.3
     *     2015-01-05 15:28 | 13.8
     *     2015-01-05 17:08 | 27.4
     * }
     * </pre>
     *
     * and a second time series:
     *
     * <pre>
     * {@code
     * second =
     *     2015-01-05 15:08 | 999.0
     *     2015-01-05 15:28 | 666.6
     * }
     * </pre>
     *
     * calling
     *
     * <pre>
     * {@code
     * first.set(second);
     * }
     * </pre>
     *
     * will set the values of first to
     *
     * <pre>
     * {@code
     * first =
     *     2015-01-05 12:55 | 24.6
     *     2015-01-05 12:59 | 28.7
     *     2015-01-05 15:08 | 999.0
     *     2015-01-05 15:28 | 666.6
     *     2015-01-05 17:08 | 27.4
     * }
     * </pre>
     *
     * Note that the other time series must be contained in this time series.
     *
     * @param that The other time series.
     * @param skipNaNs Skip NaN values.
     */
    public void set(TimeSeries<T> that, boolean skipNaNs) {
        for (int i = 0; i < that.size(); ++i) {
            int j = this.datetimes.indexOf(that.getReading(i).getDatetime());
            T value = that.values.get(i);
            if (skipNaNs && value instanceof Double && ((Double) value).isNaN()) {
                // Skip
            } else {
                this.values.set(j, value);
            }
        }
    }

    /**
     * Compute the element-wise difference between two time series.
     *
     * @param <S> The type of values.
     * @param left A time series.
     * @param right A time series.
     * @return A time series containing the difference between the two input
     * time series.
     */
    public static <S extends Number> TimeSeries<Double> computeDifference(
            TimeSeries<S> left, TimeSeries<S> right) {
        final int n = left.size();
        assert n == right.size();

        TimeSeries<Double> result = new TimeSeries<>(n, Double.class);
        for (int i = 0; i < n; ++i) {
            final Reading<S> a = left.getReading(i);
            final Reading<S> b = right.getReading(i);
            final Reading<Double> difference = new Reading<>(
                    a.getDatetime(),
                    a.getValue().doubleValue() - b.getValue().doubleValue(),
                    Double.class);
            result.addReading(difference);
        }
        return result;
    }

    /**
     * Resample a time series keeping the same starting/ending times.
     *
     * @param duration The new step.
     * @return A time series obtained by interpolating the original time series
     * with a new step.
     */
    public TimeSeries<Number> resample(Duration duration) {
        Instant startTime = datetimes.get(0);
        Instant stopTime = datetimes.get(datetimes.size() - 1);

        return resample(startTime, stopTime, duration);
    }

    /**
     * Resample a time series keeping the same starting/ending times.
     *
     * @param step The new step in seconds.
     * @return A time series obtained by interpolating the original time series
     * with a new step.
     */
    public TimeSeries<Number> resample(long step) {
        return resample(Duration.ofSeconds(step));
    }

    /**
     * Resample a time series.
     *
     * @param startTime New starting time (must be within the original time
     * series).
     * @param stopTime New ending time (must be within the original time
     * series).
     * @param step The new step.
     * @return A time series obtained by interpolating the original time series
     * with a new starting/ending time and a new step.
     */
    public TimeSeries<Number> resample(
            Instant startTime, Instant stopTime, Duration step) {
        Measure measure = new Measure("", Double.class);
        DataFrame dataframe = new DataFrame(this.datetimes);
        dataframe.addColumn(measure,
                this.values.stream()
                .map(Number::doubleValue)
                .collect(Collectors.toList()));
        DataFrame resampled = dataframe.resample(startTime, stopTime, step);
        return resampled.getColumn(measure);
    }

    public TimeSeries<Integer> toInteger() {
        List<Integer> integerValues = this.values.stream()
                .map(i -> i.intValue())
                .collect(Collectors.toList());
        TimeSeries<Integer> result = new TimeSeries<>(
                this.datetimes, integerValues, Integer.class);
        return result;
    }

    /**
     * Access the time series as a stream of readings.
     *
     * @return A stream of readings.
     */
    public Stream<Reading<T>> stream() {
        return StreamSupport.stream(this.spliterator(), true);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + Objects.hashCode(this.datetimes);
        hash = 61 * hash + Objects.hashCode(this.values);
        hash = 61 * hash + Objects.hashCode(this.clazz);
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
        final TimeSeries<?> other = (TimeSeries<?>) obj;
        if (!Objects.equals(this.datetimes, other.datetimes)) {
            return false;
        }
        if (!Objects.equals(this.values, other.values)) {
            return false;
        }
        if (!Objects.equals(this.clazz, other.clazz)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("[ ");
        for (int i = 0; i < this.datetimes.size(); ++i) {
            buffer.append(this.datetimes.get(i));
            buffer.append(": ");
            buffer.append(this.values.get(i));
            buffer.append(",\n  ");
        }
        buffer.append("]");
        return buffer.toString();
    }

    @Override
    public Iterator<Reading<T>> iterator() {
        return new Iterator<Reading<T>>() {
            @Override
            public boolean hasNext() {
                return datetimesIterator.hasNext();
            }

            @Override
            public Reading<T> next() {
                Reading<T> reading = new Reading(
                        datetimesIterator.next(),
                        valuesIterator.next(),
                        clazz
                );
                return reading;
            }

            private final Iterator<Instant> datetimesIterator = datetimes.iterator();
            private final Iterator valuesIterator = values.iterator();
        };
    }

    private List<Instant> datetimes;
    private List<T> values;
    private final Class<T> clazz;
}
