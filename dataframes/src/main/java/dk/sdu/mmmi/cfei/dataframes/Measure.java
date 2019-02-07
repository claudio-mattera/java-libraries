package dk.sdu.mmmi.cfei.dataframes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a measure, i.e., a name and a unit.
 *
 * For instance: - name: Temperature, unit: C - name: Air flow, unit: m3 / h
 *
 * @author cgim
 */
public class Measure {

    public Measure(String name, Class type, Map<String, String> metadata) {
        this.name = name;
        this.type = type;
        this.metadata = metadata;
    }

    public Measure(String name, Class type) {
        this.name = name;
        this.type = type;
        this.metadata = new HashMap<>();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.name);
        hash = 17 * hash + Objects.hashCode(this.type);
        hash = 17 * hash + Objects.hashCode(this.metadata);
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
        final Measure other = (Measure) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.type, other.type)) {
            return false;
        }
        if (!Objects.equals(this.metadata, other.metadata)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(name);
        builder.append(" (");
        builder.append(type.getName());
        builder.append(')');
        return builder.toString();
    }

    public final String name;
    public final Class type;
    public final Map<String, String> metadata;
}
