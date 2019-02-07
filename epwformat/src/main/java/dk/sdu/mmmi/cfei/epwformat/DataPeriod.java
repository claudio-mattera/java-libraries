package dk.sdu.mmmi.cfei.epwformat;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Objects;

class DataPeriod {

    public DataPeriod(int recordsPerHour, String periodName, DayOfWeek startDay, LocalDate startDate, LocalDate endDate) {
        this.recordsPerHour = recordsPerHour;
        this.periodName = periodName;
        this.startDay = startDay;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + this.recordsPerHour;
        hash = 67 * hash + Objects.hashCode(this.periodName);
        hash = 67 * hash + Objects.hashCode(this.startDay);
        hash = 67 * hash + Objects.hashCode(this.startDate);
        hash = 67 * hash + Objects.hashCode(this.endDate);
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
        final DataPeriod other = (DataPeriod) obj;
        if (this.recordsPerHour != other.recordsPerHour) {
            return false;
        }
        if (!Objects.equals(this.periodName, other.periodName)) {
            return false;
        }
        if (this.startDay != other.startDay) {
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
        String dayName = startDay.toString().toLowerCase();
        return recordsPerHour + "," + periodName + ","
                + dayName.substring(0, 1).toUpperCase() + dayName.substring(1)
                + "," + startDate + "," + endDate;
    }

    public final int recordsPerHour;
    public final String periodName;
    public final DayOfWeek startDay;
    public final LocalDate startDate;
    public final LocalDate endDate;
}
