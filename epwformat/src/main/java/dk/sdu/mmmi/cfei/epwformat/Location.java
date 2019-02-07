package dk.sdu.mmmi.cfei.epwformat;

import java.util.Objects;

class Location {

    public Location(String city, String state, String country, String dataSource, String wmoNumber, double latitude, double longitude, double timeZone, double elevation) {
        this.city = city;
        this.state = state;
        this.country = country;
        this.dataSource = dataSource;
        this.wmoNumber = wmoNumber;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timeZone = timeZone;
        this.elevation = elevation;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.city);
        hash = 53 * hash + Objects.hashCode(this.state);
        hash = 53 * hash + Objects.hashCode(this.country);
        hash = 53 * hash + Objects.hashCode(this.dataSource);
        hash = 53 * hash + Objects.hashCode(this.wmoNumber);
        hash = 53 * hash + Objects.hashCode(this.latitude);
        hash = 53 * hash + Objects.hashCode(this.longitude);
        hash = 53 * hash + Objects.hashCode(this.timeZone);
        hash = 53 * hash + Objects.hashCode(this.elevation);
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
        final Location other = (Location) obj;
        if (!Objects.equals(this.city, other.city)) {
            return false;
        }
        if (!Objects.equals(this.state, other.state)) {
            return false;
        }
        if (!Objects.equals(this.country, other.country)) {
            return false;
        }
        if (!Objects.equals(this.dataSource, other.dataSource)) {
            return false;
        }
        if (!Objects.equals(this.wmoNumber, other.wmoNumber)) {
            return false;
        }
        if (!Objects.equals(this.latitude, other.latitude)) {
            return false;
        }
        if (!Objects.equals(this.longitude, other.longitude)) {
            return false;
        }
        if (!Objects.equals(this.timeZone, other.timeZone)) {
            return false;
        }
        if (!Objects.equals(this.elevation, other.elevation)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return city + "," + state + "," + country + "," + dataSource + ","
                + wmoNumber + "," + latitude + "," + longitude + ","
                + timeZone + "," + elevation;
    }

    public final String city;
    public final String state;
    public final String country;
    public final String dataSource;
    public final String wmoNumber;
    public final double latitude;
    public final double longitude;
    public final double timeZone;
    public final double elevation;
}
