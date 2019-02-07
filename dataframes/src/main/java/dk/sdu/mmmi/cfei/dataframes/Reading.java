package dk.sdu.mmmi.cfei.dataframes;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a reading at a given datetime.
 *
 * <pre>
 * {@code
 * 2015-01-05 12:55 | 24.6
 * }
 * </pre>
 *
 * @author cgim
 */
public class Reading<T> {

    /**
     * Create a reading from a datetime and a value.
     *
     * @param datetime The datetime.
     * @param value The values.
     * @param clazz Value type.
     */
    public Reading(Instant datetime, T value, Class<T> clazz) {
        this.datetime = datetime;
        this.value = value;
        this.clazz = clazz;
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
     * Return the value.
     *
     * @return The value.
     */
    public T getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 43 * hash + Objects.hashCode(this.datetime);
        hash = 43 * hash + Objects.hashCode(this.value);
		hash = 43 * hash + Objects.hashCode(this.clazz);
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
        final Reading<?> other = (Reading<?>) obj;
        if (this.datetime != other.datetime) {
            return false;
        }
        if (!Objects.equals(this.value, other.value)) {
            return false;
        }
        if (!Objects.equals(this.clazz, other.clazz)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Reading{" + "datetime=" + datetime + ", value=" + value + '}';
    }

    private final Instant datetime;
    private final T value;
    private final Class<T> clazz;
}
