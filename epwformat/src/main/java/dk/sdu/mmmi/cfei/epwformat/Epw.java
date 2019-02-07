package dk.sdu.mmmi.cfei.epwformat;

import dk.sdu.mmmi.cfei.dataframes.DataFrame;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A representation of an EPW weather file.
 *
 * @see EpwParser
 * @see
 * <a href="http://bigladdersoftware.com/epx/docs/8-2/auxiliary-programs/epw-csv-format-inout.html">EPW
 * CSV Format (In/Out)</a>
 * @see
 * <a href="http://www.cambeep.eng.cam.ac.uk/References/weatherdata">Weather
 * format</a>
 *
 * @author cgim
 */
public class Epw {

    /**
     * Export to an EPW file.
     *
     * @return A valid EPW file content.
     */
    public String export() {
        StringBuilder builder = new StringBuilder();

        builder.append("LOCATION,");
        builder.append(location.toString());
        builder.append('\n');

        builder.append("DESIGN CONDITIONS,");
        builder.append(designConditions.toString());
        builder.append('\n');

        builder.append("TYPICAL/EXTREME PERIODS,");
        builder.append(typicalOrExtremePeriods.size());
        builder.append(',');
        builder.append(typicalOrExtremePeriods.stream()
                .map(TypicalOrExtremePeriod::toString)
                .collect(Collectors.joining(",")));
        builder.append('\n');

        builder.append("GROUND TEMPERATURES,");
        builder.append(groundTemperatures.size());
        builder.append(',');
        builder.append(groundTemperatures.stream()
                .map(GroundTemperature::toString)
                .collect(Collectors.joining(",")));
        builder.append('\n');

        builder.append("HOLIDAYS/DAYLIGHT SAVINGS,");
        builder.append(holidaysOrDaylightSavings.toString());
        builder.append('\n');

        comments.forEach(comment -> {
            builder.append("COMMENTS ");
            builder.append(comment.number);
            builder.append(',');
            builder.append(comment.comment);
            builder.append('\n');
        });
        builder.append("DATA PERIODS,");
        builder.append(dataPeriods.size());
        builder.append(',');
        builder.append(dataPeriods.stream()
                .map(DataPeriod::toString)
                .collect(Collectors.joining(",")));
        builder.append('\n');

        DateTimeFormatter formatter
                = DateTimeFormatter.ofPattern("yyyy,MM,dd,");

        builder.append(
                dataframe.stream().map(row -> {
                    Instant datetime = row.getDatetime();
                    LocalDateTime localDatetime = datetime.atZone(zone).toLocalDateTime();
                    String line = row.stream().map(Epw::objectToString)
                            .collect(Collectors.joining(","));
                    return localDatetime.format(formatter)
                            + String.format("%02d", localDatetime.getHour() + 1)
                            + ",60,"
                            + line;
                }).collect(Collectors.joining("\n")));

        return builder.toString();
    }

    public static Class getDatatypeForField(String field) {
        final int index = EpwParser.COLUMN_NAMES.entrySet().stream()
                .filter(e -> e.getValue().equals(field))
                .map(Map.Entry::getKey)
                .findFirst()
                .get();
        final Optional<Class> datatype = Optional.ofNullable(
                EpwParser.COLUMN_TYPES.get(index));
        return datatype.get();
    }

    Location getLocation() {
        return location;
    }

    void setLocation(Location location) {
        this.location = location;
    }

    DesignConditions getDesignConditions() {
        return designConditions;
    }

    void setDesignConditions(DesignConditions designConditions) {
        this.designConditions = designConditions;
    }

    List<TypicalOrExtremePeriod> getTypicalOrExtremePeriods() {
        return typicalOrExtremePeriods;
    }

    void setTypicalOrExtremePeriods(List<TypicalOrExtremePeriod> typicalOrExtremePeriods) {
        this.typicalOrExtremePeriods = typicalOrExtremePeriods;
    }

    List<GroundTemperature> getGroundTemperatures() {
        return groundTemperatures;
    }

    void setGroundTemperatures(List<GroundTemperature> groundTemperatures) {
        this.groundTemperatures = groundTemperatures;
    }

    HolidaysOrDaylightSavings getHolidaysOrDaylightSavings() {
        return holidaysOrDaylightSavings;
    }

    void setHolidaysOrDaylightSavings(HolidaysOrDaylightSavings holidaysOrDaylightSavings) {
        this.holidaysOrDaylightSavings = holidaysOrDaylightSavings;
    }

    List<Comments> getComments() {
        return comments;
    }

    void setComments(List<Comments> comments) {
        this.comments = comments;
    }

    List<DataPeriod> getDataPeriods() {
        return dataPeriods;
    }

    void setDataPeriods(List<DataPeriod> dataPeriods) {
        this.dataPeriods = dataPeriods;
    }

    DataFrame getDataframe() {
        return dataframe;
    }

    void setDataframe(DataFrame dataframe) {
        this.dataframe = dataframe;
    }

    public ZoneId getZone() {
        return zone;
    }

    public void setZone(ZoneId zone) {
        this.zone = zone;
    }

    /**
     * Modify the underlying data frame.
     *
     * @param modifier A function that modifies the underlying data frame.
     */
    public void modifyDataframe(Consumer<DataFrame> modifier) {
        modifier.accept(this.dataframe);
    }

    static String doubleToString(Double value) {
        if (Double.isNaN(value)) {
            return "";
        } else {
            return String.valueOf(value);
        }
    }

    static String objectToString(Object obj) {
        if (obj instanceof Double && Double.isNaN((Double) obj)) {
            return "";
        } else {
            return String.valueOf(obj);
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + Objects.hashCode(this.location);
        hash = 43 * hash + Objects.hashCode(this.designConditions);
        hash = 43 * hash + Objects.hashCode(this.typicalOrExtremePeriods);
        hash = 43 * hash + Objects.hashCode(this.groundTemperatures);
        hash = 43 * hash + Objects.hashCode(this.holidaysOrDaylightSavings);
        hash = 43 * hash + Objects.hashCode(this.comments);
        hash = 43 * hash + Objects.hashCode(this.dataPeriods);
        hash = 43 * hash + Objects.hashCode(this.dataframe);
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
        final Epw other = (Epw) obj;
        if (!Objects.equals(this.location, other.location)) {
            return false;
        }
        if (!Objects.equals(this.designConditions, other.designConditions)) {
            return false;
        }
        if (!Objects.equals(this.typicalOrExtremePeriods, other.typicalOrExtremePeriods)) {
            return false;
        }
        if (!Objects.equals(this.groundTemperatures, other.groundTemperatures)) {
            return false;
        }
        if (!Objects.equals(this.holidaysOrDaylightSavings, other.holidaysOrDaylightSavings)) {
            return false;
        }
        if (!Objects.equals(this.comments, other.comments)) {
            return false;
        }
        if (!Objects.equals(this.dataPeriods, other.dataPeriods)) {
            return false;
        }
        if (!Objects.equals(this.dataframe, other.dataframe)) {
            return false;
        }
        return true;
    }

    private Location location;
    private DesignConditions designConditions;
    private List<TypicalOrExtremePeriod> typicalOrExtremePeriods;
    private List<GroundTemperature> groundTemperatures;
    private HolidaysOrDaylightSavings holidaysOrDaylightSavings;
    private List<Comments> comments;
    private List<DataPeriod> dataPeriods;
    private DataFrame dataframe;
    private ZoneId zone;
}
