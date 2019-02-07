package dk.sdu.mmmi.cfei.epwformat;

import java.time.LocalDate;
import java.util.Objects;

class TypicalOrExtremePeriod {

    public TypicalOrExtremePeriod(String name, Type type, LocalDate startDate, LocalDate endDate) {
        this.name = name;
        this.type = type;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.type);
        hash = 59 * hash + Objects.hashCode(this.startDate);
        hash = 59 * hash + Objects.hashCode(this.endDate);
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
        final TypicalOrExtremePeriod other = (TypicalOrExtremePeriod) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (this.type != other.type) {
            return false;
        }
        if (!Objects.equals(this.startDate, other.startDate)) {
            return false;
        }
        if (!Objects.equals(this.endDate, other.endDate)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        String typeString = type.toString().toLowerCase();
        return name + "," + typeString.substring(0, 1).toUpperCase()
                + typeString.substring(1) + "," + startDate + "," + endDate;
    }

    public final String name;
    public final Type type;
    public final LocalDate startDate;
    public final LocalDate endDate;

    public enum Type {
        TYPICAL,
        EXTREME
    }
}
