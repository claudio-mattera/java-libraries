package dk.sdu.mmmi.cfei.epwformat;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class HolidaysOrDaylightSavings {

    public HolidaysOrDaylightSavings(Observed leapYearObserved, int daylightSavingsStartDay, int daylightSavingsEndDay, int numberOfHolidayDefinitions, List<Holiday> holidays) {
        this.leapYearObserved = leapYearObserved;
        this.daylightSavingsStartDay = daylightSavingsStartDay;
        this.daylightSavingsEndDay = daylightSavingsEndDay;
        this.numberOfHolidayDefinitions = numberOfHolidayDefinitions;
        this.holidays = holidays;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.leapYearObserved);
        hash = 37 * hash + this.daylightSavingsStartDay;
        hash = 37 * hash + this.daylightSavingsEndDay;
        hash = 37 * hash + this.numberOfHolidayDefinitions;
        hash = 37 * hash + Objects.hashCode(this.holidays);
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
        final HolidaysOrDaylightSavings other = (HolidaysOrDaylightSavings) obj;
        if (this.daylightSavingsStartDay != other.daylightSavingsStartDay) {
            return false;
        }
        if (this.daylightSavingsEndDay != other.daylightSavingsEndDay) {
            return false;
        }
        if (this.numberOfHolidayDefinitions != other.numberOfHolidayDefinitions) {
            return false;
        }
        if (this.leapYearObserved != other.leapYearObserved) {
            return false;
        }
        if (!Objects.equals(this.holidays, other.holidays)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        String leapYearObservedString = leapYearObserved.toString().toLowerCase();
        return leapYearObservedString.substring(0, 1).toUpperCase()
                + leapYearObservedString.substring(1) + "," + daylightSavingsStartDay + ","
                + daylightSavingsEndDay + "," + numberOfHolidayDefinitions + ","
                + holidays.stream()
                .map(Holiday::toString)
                .collect(Collectors.joining(","));
    }

    public final Observed leapYearObserved;
    public final int daylightSavingsStartDay;
    public final int daylightSavingsEndDay;
    public final int numberOfHolidayDefinitions;
    public final List<Holiday> holidays;

    public enum Observed {
        YES,
        NO
    }

    public static class Holiday {

        public Holiday(String name, LocalDate date) {
            this.name = name;
            this.date = date;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 79 * hash + Objects.hashCode(this.name);
            hash = 79 * hash + Objects.hashCode(this.date);
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
            final Holiday other = (Holiday) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (!Objects.equals(this.date, other.date)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return name + "," + date;
        }

        public final String name;
        public final LocalDate date;
    }
}
