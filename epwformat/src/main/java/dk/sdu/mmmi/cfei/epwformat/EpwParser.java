package dk.sdu.mmmi.cfei.epwformat;

import dk.sdu.mmmi.cfei.dataframes.DataFrame;
import dk.sdu.mmmi.cfei.dataframes.Measure;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A parser for EPW weather files.
 *
 * @see Epw
 *
 * @author cgim
 */
public class EpwParser {

    /**
     * Create a new parser.
     *
     * An EPW file contains one data point per each hour in one year, but the
     * year itself is not relevant (e.g., there might be data points in year
     * 2012 followed by data points in year 1970, as long as month, day and hour
     * are correct). This parser parses all dates to a given "base" year.
     *
     * While time zone information might be extracted from the Location line,
     * this is overridden with the `zone` parameter.
     *
     * @param year Base year.
     * @param zone Default time zone
     */
    public EpwParser(int year, ZoneId zone) {
        this.YEAR = year;
        this.zone = zone;
    }

    /**
     * Create a new parser (zone is set to UTC).
     *
     * @param year Base year.
     */
    public EpwParser(int year) {
        this(year, ZoneOffset.UTC);
    }

    /**
     * Create a new parser (year is set to 2000).
     */
    public EpwParser() {
        this(2000);
    }

    /**
     * Parse an EPW weather file.
     *
     * @param content The weather file content.
     * @return An EPW structure.
     */
    public Epw parse(String content) {
        Epw epw = new Epw();
        List<String> lines = Arrays.asList(content.split("\n"));
        parseLocation(lines.get(0)).ifPresent(epw::setLocation);
        parseDesignConditions(lines.get(1)).ifPresent(epw::setDesignConditions);
        epw.setTypicalOrExtremePeriods(parseTypicalOrExtremePeriods(lines.get(2)));
        epw.setGroundTemperatures(parseGroundTemperature(lines.get(3)));
        parseHolidaysOrDaylightSavings(lines.get(4)).ifPresent(epw::setHolidaysOrDaylightSavings);
        epw.setComments(Arrays.asList(parseComment(lines.get(5)).get(), parseComment(lines.get(6)).get()));
        epw.setDataPeriods(parseDataPeriods(lines.get(7)));
        epw.setDataframe(parseData(lines.subList(8, lines.size())));
        epw.setZone(zone);
        return epw;
    }

    public static Class getDType(String fieldName) {
        int key = COLUMN_NAMES.entrySet().stream()
                .filter(entry -> entry.getValue().equals(fieldName))
                .findAny().get()
                .getKey();
        return COLUMN_TYPES.get(key);
    }

    DataFrame parseData(List<String> lines) {
        // Date format: yyyy,MM,dd,HH,60
        // where HH is in range [1, 24]
        // Year is ignored, data is sorted by month, day, hour
        // (year can be non-increasing)

        final int expectedDataFieldsCount = lines.size();

        Map<Integer, List> values = new HashMap();

        Consumer<String> parseSingleLine = (line -> {
            List<String> allFields = Arrays.asList(line.trim().split(","));
            List<String> fields = allFields.subList(5, allFields.size());

            for (Integer i = 0; i < fields.size(); ++i) {
                Class type = COLUMN_TYPES.get(i);
                if (!values.containsKey(i)) {
                    List list;
                    if (type == String.class) {
                        list = new ArrayList<String>(expectedDataFieldsCount);
                    } else if (type == Integer.class) {
                        list = new ArrayList<Integer>(expectedDataFieldsCount);
                    } else if (type == Double.class) {
                        list = new ArrayList<Double>(expectedDataFieldsCount);
                    } else {
                        throw new RuntimeException("Unknown column type");
                    }
                    values.put(i, list);
                }

                if (type == String.class) {
                    values.get(i).add(fields.get(i));
                } else if (type == Integer.class) {
                    int value;
                    try {
                        value = Integer.valueOf(fields.get(i));
                    } catch (NumberFormatException e) {
                        value = Double.valueOf(fields.get(i)).intValue();
                    }
                    values.get(i).add(value);
                } else if (type == Double.class) {
                    values.get(i).add(Double.valueOf(fields.get(i)));
                } else {
                    throw new RuntimeException("Unknown column type");
                }
            }
        });

        final int twentyEighthFebruaryIndex = 24 * (31 + 27);
        final int twentyNinthFebruaryIndex = 24 * (31 + 28);
        final boolean isLeap = Year.isLeap(YEAR);
        if (lines.size() > twentyNinthFebruaryIndex) {
            final boolean hasLeap = expectedDataFieldsCount > twentyNinthFebruaryIndex
                    ? lines.get(twentyNinthFebruaryIndex).trim().split(",")[2].equals("29")
                    : false;

            for (String line : lines.subList(0, twentyNinthFebruaryIndex)) {
                parseSingleLine.accept(line);
            }
            if (isLeap && !hasLeap) {
                // this.YEAR is a leap year, but the EPW file does not have data
                // from 29th February. Let's just replicate the 28th February.
                for (String line : lines.subList(twentyEighthFebruaryIndex, twentyNinthFebruaryIndex)) {
                    parseSingleLine.accept(line.replace("02,28,60", "02,29,60"));
                }
            }
            for (String line : lines.subList(twentyNinthFebruaryIndex, lines.size())) {
                parseSingleLine.accept(line);
            }
        } else {
            for (String line : lines) {
                parseSingleLine.accept(line);
            }
        }

        List<Instant> datetimes = IntStream.range(0, expectedDataFieldsCount)
                .mapToObj(i -> LocalDateTime.of(YEAR, Month.JANUARY, 1, 0, 0).plusHours(i))
                .map(datetime -> datetime.atZone(zone).toInstant())
                .collect(Collectors.toList());

        DataFrame dataframe = new DataFrame(datetimes);

        values.entrySet().forEach(entry -> {
            Measure measure = new Measure(
                    COLUMN_NAMES.get(entry.getKey()),
                    COLUMN_TYPES.get(entry.getKey()));
            dataframe.addColumn(measure, entry.getValue());
        });

        return dataframe;
    }

    Optional<Location> parseLocation(String line) {
        Pattern pattern = Pattern.compile(
                "^LOCATION,"
                + "(?<city>.+?),"
                + "(?<state>.+?),"
                + "(?<country>.+?),"
                + "(?<dataSource>.+?),"
                + "(?<wmoNumber>.+?),"
                + "(?<latitude>.+?),"
                + "(?<longitude>.+?),"
                + "(?<timeZone>.+?),"
                + "(?<elevation>.+?)$");
        Matcher matcher = pattern.matcher(line.trim());
        if (matcher.matches()) {
            String city = matcher.group("city");
            String state = matcher.group("state");
            String country = matcher.group("country");
            String dataSource = matcher.group("dataSource");
            String wmoNumber = matcher.group("wmoNumber");
            double latitude = Double.valueOf(matcher.group("latitude"));
            double longitude = Double.valueOf(matcher.group("longitude"));
            double timeZone = Double.valueOf(matcher.group("timeZone"));
            double elevation = Double.valueOf(matcher.group("elevation"));
            return Optional.of(new Location(
                    city,
                    state,
                    country,
                    dataSource,
                    wmoNumber,
                    latitude,
                    longitude,
                    timeZone,
                    elevation));
        } else {
            return Optional.empty();
        }
    }

    List<DataPeriod> parseDataPeriods(String line) {
        Pattern outerPattern = Pattern.compile(
                "^DATA PERIODS,"
                + "(?<numberOfPeriods>.+?),"
                + "(?<periods>.+)$");
        Pattern innerPattern = Pattern.compile(
                "(?<recordsPerHour>.+?),"
                + "(?<periodName>.+?),"
                + "(?<startDay>.+?),"
                + "(?<startDate>.+?),"
                + "(?<endDate>[^,]+),?");
        Matcher outerMatcher = outerPattern.matcher(line.trim());
        if (outerMatcher.matches()) {
            int numberOfPeriods = Integer.valueOf(
                    outerMatcher.group("numberOfPeriods"));
            String rest = outerMatcher.group("periods");

            List<DataPeriod> periods = new ArrayList<>();
            Matcher innerMatcher = innerPattern.matcher(rest);
            while (innerMatcher.find()) {
                int recordsPerHour = Integer.valueOf(
                        innerMatcher.group("recordsPerHour"));
                String periodName = innerMatcher.group("periodName");
                DayOfWeek startDay = DayOfWeek.valueOf(
                        innerMatcher.group("startDay").toUpperCase());
                LocalDate startDate = parseDate(innerMatcher.group("startDate"));
                LocalDate endDate = parseDate(innerMatcher.group("endDate"));
                periods.add(new DataPeriod(
                        recordsPerHour, periodName, startDay, startDate, endDate));
            }
            return periods;
        } else {
            return Arrays.asList();
        }
    }

    List<TypicalOrExtremePeriod> parseTypicalOrExtremePeriods(String line) {
        Pattern outerPattern = Pattern.compile(
                "^TYPICAL/EXTREME PERIODS,"
                + "(?<numberOfPeriods>.+?),"
                + "(?<periods>.+)$");
        Pattern innerPattern = Pattern.compile(
                "(?<periodName>.+?),"
                + "(?<type>(Typical|Extreme)),"
                + "(?<startDate>.+?),"
                + "(?<endDate>[^,]+),?");
        Matcher outerMatcher = outerPattern.matcher(line.trim());
        if (outerMatcher.matches()) {
            int numberOfPeriods = Integer.valueOf(
                    outerMatcher.group("numberOfPeriods"));
            String rest = outerMatcher.group("periods");

            List<TypicalOrExtremePeriod> periods = new ArrayList<>();
            Matcher innerMatcher = innerPattern.matcher(rest);
            while (innerMatcher.find()) {
                String periodName = innerMatcher.group("periodName");
                TypicalOrExtremePeriod.Type type
                        = TypicalOrExtremePeriod.Type.valueOf(
                                innerMatcher.group("type").toUpperCase());
                LocalDate startDate = parseDate(innerMatcher.group("startDate"));
                LocalDate endDate = parseDate(innerMatcher.group("endDate"));
                periods.add(new TypicalOrExtremePeriod(
                        periodName, type, startDate, endDate));
            }
            return periods;
        } else {
            return Arrays.asList();
        }
    }

    List<GroundTemperature> parseGroundTemperature(String line) {
        Pattern outerPattern = Pattern.compile(
                "^GROUND TEMPERATURES,"
                + "(?<numberOfTemperatures>.+?),"
                + "(?<temperatures>.+)$");
        Matcher outerMatcher = outerPattern.matcher(line.trim());
        final int COUNT = 16;
        if (outerMatcher.matches()) {
            int numberOfTemperatures
                    = Integer.valueOf(outerMatcher.group("numberOfTemperatures"));
            String[] rest = outerMatcher.group("temperatures").split(",");

            List<GroundTemperature> temperatures = new ArrayList<>();
            for (int i = 0; i < numberOfTemperatures; ++i) {
                double groundTemperatureDepth = parseDouble(rest[i * COUNT + 0]);
                double depthSoilConductivity = parseDouble(rest[i * COUNT + 1]);
                double depthSoilDensity = parseDouble(rest[i * COUNT + 2]);
                double depthSoilSpecificHeat = parseDouble(rest[i * COUNT + 3]);
                double depthJanuaryAverageGroundTemperature = parseDouble(rest[i * COUNT + 4]);
                double depthFebruaryAverageGroundTemperature = parseDouble(rest[i * COUNT + 5]);
                double depthMarchAverageGroundTemperature = parseDouble(rest[i * COUNT + 6]);
                double depthAprilAverageGroundTemperature = parseDouble(rest[i * COUNT + 7]);
                double depthMayAverageGroundTemperature = parseDouble(rest[i * COUNT + 8]);
                double depthJuneAverageGroundTemperature = parseDouble(rest[i * COUNT + 9]);
                double depthJulyAverageGroundTemperature = parseDouble(rest[i * COUNT + 10]);
                double depthAugustAverageGroundTemperature = parseDouble(rest[i * COUNT + 11]);
                double depthSeptemberAverageGroundTemperature = parseDouble(rest[i * COUNT + 12]);
                double depthOctoberAverageGroundTemperature = parseDouble(rest[i * COUNT + 13]);
                double depthNovemberAverageGroundTemperature = parseDouble(rest[i * COUNT + 14]);
                double depthDecemberAverageGroundTemperature = parseDouble(rest[i * COUNT + 15]);
                temperatures.add(new GroundTemperature(
                        groundTemperatureDepth,
                        depthSoilConductivity,
                        depthSoilDensity,
                        depthSoilSpecificHeat,
                        depthJanuaryAverageGroundTemperature,
                        depthFebruaryAverageGroundTemperature,
                        depthMarchAverageGroundTemperature,
                        depthAprilAverageGroundTemperature,
                        depthMayAverageGroundTemperature,
                        depthJuneAverageGroundTemperature,
                        depthJulyAverageGroundTemperature,
                        depthAugustAverageGroundTemperature,
                        depthSeptemberAverageGroundTemperature,
                        depthOctoberAverageGroundTemperature,
                        depthNovemberAverageGroundTemperature,
                        depthDecemberAverageGroundTemperature));
            }
            return temperatures;
        } else {
            return Arrays.asList();
        }
    }

    Optional<Comments> parseComment(String line) {
        Pattern pattern = Pattern.compile(
                "^COMMENTS (?<number>\\d+),(?<comment>.+)$");
        Matcher matcher = pattern.matcher(line.trim());
        if (matcher.matches()) {
            int number = Integer.valueOf(matcher.group("number"));
            String comment = matcher.group("comment");
            return Optional.of(new Comments(number, comment));
        } else {
            return Optional.empty();
        }
    }

    Optional<DesignConditions> parseDesignConditions(String line) {
        Pattern pattern = Pattern.compile("^DESIGN CONDITIONS,(?<text>.+)$");
        Matcher matcher = pattern.matcher(line.trim());
        if (matcher.matches()) {
            String data = matcher.group("text");
            return Optional.of(new DesignConditions(data));
        } else {
            return Optional.empty();
        }
    }

    Optional<HolidaysOrDaylightSavings> parseHolidaysOrDaylightSavings(String line) {
        Pattern outerPattern = Pattern.compile(
                "^HOLIDAYS\\/DAYLIGHT SAVINGS,"
                + "(?<leapYearObserved>(Yes|No)),"
                + "(?<daylightSavingsStartDay>.+?),"
                + "(?<daylightSavingsEndDay>.+?),"
                + "(?<numberOfHolidayDefinitions>.+?),?"
                + "(?<holidays>.*?)$");
        Matcher outerMatcher = outerPattern.matcher(line.trim());
        if (outerMatcher.matches()) {
            HolidaysOrDaylightSavings.Observed observed
                    = HolidaysOrDaylightSavings.Observed.valueOf(
                            outerMatcher.group("leapYearObserved").toUpperCase());
            int daylightSavingsStartDay
                    = Integer.valueOf(
                            outerMatcher.group("daylightSavingsStartDay"));
            int daylightSavingsEndDay
                    = Integer.valueOf(
                            outerMatcher.group("daylightSavingsEndDay"));
            int numberOfHolidayDefinitions
                    = Integer.valueOf(
                            outerMatcher.group("numberOfHolidayDefinitions"));
            String[] rest = outerMatcher.group("holidays").split(",");

            List<HolidaysOrDaylightSavings.Holiday> holidays = new ArrayList<>();
            for (int i = 0; i < numberOfHolidayDefinitions; ++i) {
                String name = rest[i * 2 + 0];
                LocalDate date = parseDate(rest[i * 2 + 1]);
                holidays.add(new HolidaysOrDaylightSavings.Holiday(name, date));
            }
            return Optional.of(
                    new HolidaysOrDaylightSavings(
                            observed,
                            daylightSavingsStartDay,
                            daylightSavingsEndDay,
                            numberOfHolidayDefinitions,
                            holidays));
        } else {
            return Optional.empty();
        }
    }

    private static double parseDouble(String value) {
        if (value.isEmpty()) {
            return Double.NaN;
        } else {
            return Double.valueOf(value);
        }
    }

    private LocalDate parseDate(String string) {
        try {
            return LocalDate.parse(string);
        } catch (DateTimeParseException ex) {
            List<Integer> fields = Arrays.stream(string.split("/"))
                    .map(String::trim)
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
            int month = fields.get(0);
            int day = fields.get(1);
            return LocalDate.of(YEAR, month, day);
        }
    }

    private final int YEAR;
    private ZoneId zone;

    private static final int HOURS_IN_ONE_YEAR = 8760;
    static final Map<Integer, String> COLUMN_NAMES;
    static final Map<Integer, Class> COLUMN_TYPES;

    static {
        COLUMN_NAMES = new HashMap<>();
        COLUMN_NAMES.put(0, "dataSourceAndUncertaintyFlags");
        COLUMN_NAMES.put(1, "dryBulbTemp");
        COLUMN_NAMES.put(2, "dewPointTemp");
        COLUMN_NAMES.put(3, "relativeHumidity");
        COLUMN_NAMES.put(4, "atmosphericStationPressure");
        COLUMN_NAMES.put(5, "extraterrestrialHorizontalRadiation");
        COLUMN_NAMES.put(6, "extraterrestrialDirectNormalRadiation");
        COLUMN_NAMES.put(7, "horizontalInfraredRadiationFromSky");
        COLUMN_NAMES.put(8, "globalHorizontalRadiation");
        COLUMN_NAMES.put(9, "directNormalRadiation");
        COLUMN_NAMES.put(10, "diffuseHorizontalRadiation");
        COLUMN_NAMES.put(11, "globalHorizontalIlluminance");
        COLUMN_NAMES.put(12, "directNormalIlluminance");
        COLUMN_NAMES.put(13, "diffuseHorizontalIlluminance");
        COLUMN_NAMES.put(14, "zenithLuminance");
        COLUMN_NAMES.put(15, "windDirection");
        COLUMN_NAMES.put(16, "windSpeed");
        COLUMN_NAMES.put(17, "totalSkyCover");
        COLUMN_NAMES.put(18, "opaqueSkyCover");
        COLUMN_NAMES.put(19, "visibility");
        COLUMN_NAMES.put(20, "ceilingHeight");
        COLUMN_NAMES.put(21, "presentWeatherObservation");
        COLUMN_NAMES.put(22, "presentWeatherCodes");
        COLUMN_NAMES.put(23, "precipitableWater");
        COLUMN_NAMES.put(24, "aerosolOpticalDepth");
        COLUMN_NAMES.put(25, "snowDepth");
        COLUMN_NAMES.put(26, "daysSinceLastSnowfall");
        COLUMN_NAMES.put(27, "albedo");
        COLUMN_NAMES.put(28, "liquidPrecipitationDepth");
        COLUMN_NAMES.put(29, "liquidPrecipitationQuantity");
    }

    static {
        COLUMN_TYPES = new HashMap<>();
        COLUMN_TYPES.put(0, String.class);
        COLUMN_TYPES.put(1, Double.class);
        COLUMN_TYPES.put(2, Double.class);
        COLUMN_TYPES.put(3, Integer.class);
        COLUMN_TYPES.put(4, Integer.class);
        COLUMN_TYPES.put(5, Integer.class);
        COLUMN_TYPES.put(6, Integer.class);
        COLUMN_TYPES.put(7, Integer.class);
        COLUMN_TYPES.put(8, Integer.class);
        COLUMN_TYPES.put(9, Integer.class);
        COLUMN_TYPES.put(10, Integer.class);
        COLUMN_TYPES.put(11, Integer.class);
        COLUMN_TYPES.put(12, Integer.class);
        COLUMN_TYPES.put(13, Integer.class);
        COLUMN_TYPES.put(14, Integer.class);
        COLUMN_TYPES.put(15, Integer.class);
        COLUMN_TYPES.put(16, Double.class);
        COLUMN_TYPES.put(17, Integer.class);
        COLUMN_TYPES.put(18, Integer.class);
        COLUMN_TYPES.put(19, Double.class);
        COLUMN_TYPES.put(20, Integer.class);
        COLUMN_TYPES.put(21, Integer.class);
        COLUMN_TYPES.put(22, Integer.class);
        COLUMN_TYPES.put(23, Integer.class);
        COLUMN_TYPES.put(24, Double.class);
        COLUMN_TYPES.put(25, Integer.class);
        COLUMN_TYPES.put(26, Integer.class);
        COLUMN_TYPES.put(27, Double.class);
        COLUMN_TYPES.put(28, Double.class);
        COLUMN_TYPES.put(29, Double.class);
    }
}
