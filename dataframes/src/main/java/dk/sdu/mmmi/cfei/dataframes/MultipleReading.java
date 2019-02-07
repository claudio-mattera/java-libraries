package dk.sdu.mmmi.cfei.dataframes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a set of readings at a given datetime.
 *
 * <pre>
 * {@code
 * 2015-01-05 12:55 | 24.6 | 34
 * }
 * </pre>
 *
 * @author cgim
 */
public class MultipleReading implements Iterable<Object> {

    /**
     * Create a set of readings from a datetime and a list of values.
     *
     * @param datetime The datetime.
     * @param values The list of values.
     */
    public MultipleReading(Instant datetime, List values) {
        this.datetime = datetime;
        this.values = values;
    }

    /**
     * Return the datetime.
     *
     * @return A datetime.
     */
    public Instant getDatetime() {
        return datetime;
    }

    /**
     * Return the number of values.
     *
     * @return Number of values.
     */
    public int size() {
        return values.size();
    }

    /**
     * Return a value at a given index.
     *
     * @param i The index.
     * @return The value.
     */
    public Object getValue(int i) {
        return values.get(i);
    }

    /**
     * Return a value at the last index.
     *
     * @return The value.
     */
    public Object getLastValue() {
        final int index = this.size() - 1;
        return values.get(index);
    }

    /**
     * Access the set of readings as a stream of values.
     *
     * @return A stream of values.
     */
    public Stream<Object> stream() {
        return StreamSupport.stream(this.spliterator(), true);
    }

    @Override
    public Iterator<Object> iterator() {
        return new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return i < values.size();
            }

            @Override
            public Object next() {
                Object value = values.get(i);
                i += 1;
                return value;
            }
            int i = 0;
        };
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.datetime);
        hash = 41 * hash + Objects.hashCode(this.values);
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
        final MultipleReading other = (MultipleReading) obj;
        if (!Objects.equals(this.datetime, other.datetime)) {
            return false;
        }
        if (!Objects.equals(this.values, other.values)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        Stream<String> valuesStream = values.stream()
                .map(String::valueOf);
        String valuesString = valuesStream
                .collect(Collectors.joining(", "));
        return "MultipleReading{"
                + "datetime=" + datetime
                + ", values=" + valuesString + '}';
    }

    private final Instant datetime;
    private final List values;
}
