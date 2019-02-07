package dk.sdu.mmmi.cfei.epwformat;

import dk.sdu.mmmi.cfei.dataframes.DataFrame;
import dk.sdu.mmmi.cfei.dataframes.Measure;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cgim
 */
public class EpwTest {

    @Test
    public void DataToStringTest() {
        final String expected
                = "1970,01,01,01,60,B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7,-7.0,-8.5,88,83400,0,0,9999,0,0,0,0,0,0,0,230,1.5,10,8,9999.0,99999,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,02,60,A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7,-7.2,-8.3,91,83400,0,0,9999,0,0,0,0,0,0,0,220,1.5,10,8,11.3,1128,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,03,60,B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7,-8.1,-8.8,94,83400,0,0,9999,0,0,0,0,0,0,0,210,1.5,7,6,9999.0,99999,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,04,60,B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7,-9.1,-9.4,97,83400,0,0,9999,0,0,0,0,0,0,0,210,1.5,5,4,9999.0,99999,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,05,60,A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7,-10.0,-10.0,100,83300,0,0,9999,0,0,0,0,0,0,0,200,1.5,2,2,24.1,77777,0,999999999,3,0.034,3,0,0.0,0.0,0.0";

        Instant[] datetimes = {
            LocalDateTime.of(1970, Month.JANUARY, 1, 0, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 1, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 2, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 3, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 4, 0).toInstant(ZoneOffset.UTC)};

        String[] dataSourceAndUncertaintyFlags = {
            "B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7",
            "A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7",
            "B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7",
            "B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7",
            "A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7"};

        Double[] dryBulbTemp = {-7.0, -7.2, -8.1, -9.1, -10.0};
        Double[] dewPointTemp = {-8.5, -8.3, -8.8, -9.4, -10.0};
        Integer[] relativeHumidity = {88, 91, 94, 97, 100};
        Integer[] atmosphericStationPressure = {83400, 83400, 83400, 83400, 83300};
        Integer[] extraterrestrialHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] extraterrestrialDirectNormalRadiation = {0, 0, 0, 0, 0};
        Integer[] horizontalInfraredRadiationFromSky = {9999, 9999, 9999, 9999, 9999};
        Integer[] globalHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] directNormalRadiation = {0, 0, 0, 0, 0};
        Integer[] diffuseHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] globalHorizontalIlluminance = {0, 0, 0, 0, 0};
        Integer[] directNormalIlluminance = {0, 0, 0, 0, 0};
        Integer[] diffuseHorizontalIlluminance = {0, 0, 0, 0, 0};
        Integer[] zenithLuminance = {0, 0, 0, 0, 0};
        Integer[] windDirection = {230, 220, 210, 210, 200};
        Double[] windSpeed = {1.5, 1.5, 1.5, 1.5, 1.5};
        Integer[] totalSkyCover = {10, 10, 7, 5, 2};
        Integer[] opaqueSkyCover = {8, 8, 6, 4, 2};
        Double[] visibility = {9999.0, 11.3, 9999.0, 9999.0, 24.1};
        Integer[] ceilingHeight = {99999, 1128, 99999, 99999, 77777};
        Integer[] presentWeatherObservation = {0, 0, 0, 0, 0};
        Integer[] presentWeatherCodes = {999999999, 999999999, 999999999, 999999999, 999999999};
        Integer[] precipitableWater = {3, 3, 3, 3, 3};
        Double[] aerosolOpticalDepth = {0.034, 0.034, 0.034, 0.034, 0.034};
        Integer[] snowDepth = {3, 3, 3, 3, 3};
        Integer[] daysSinceLastSnowfall = {0, 0, 0, 0, 0};
        Double[] albedo = {0.0, 0.0, 0.0, 0.0, 0.0};
        Double[] liquidPrecipitationDepth = {0.0, 0.0, 0.0, 0.0, 0.0};
        Double[] liquidPrecipitationQuantity = {0.0, 0.0, 0.0, 0.0, 0.0};

        DataFrame dataframe = new DataFrame(Arrays.asList(datetimes));

        dataframe.addColumn(new Measure("dataSourceAndUncertaintyFlags", String.class), Arrays.asList(dataSourceAndUncertaintyFlags));

        dataframe.addColumn(new Measure("dryBulbTemp", dryBulbTemp.getClass().getComponentType()), Arrays.asList(dryBulbTemp));
        dataframe.addColumn(new Measure("dewPointTemp", dewPointTemp.getClass().getComponentType()), Arrays.asList(dewPointTemp));
        dataframe.addColumn(new Measure("relativeHumidity", relativeHumidity.getClass().getComponentType()), Arrays.asList(relativeHumidity));
        dataframe.addColumn(new Measure("atmosphericStationPressure", atmosphericStationPressure.getClass().getComponentType()), Arrays.asList(atmosphericStationPressure));
        dataframe.addColumn(new Measure("extraterrestrialHorizontalRadiation", extraterrestrialHorizontalRadiation.getClass().getComponentType()), Arrays.asList(extraterrestrialHorizontalRadiation));
        dataframe.addColumn(new Measure("extraterrestrialDirectNormalRadiation", extraterrestrialDirectNormalRadiation.getClass().getComponentType()), Arrays.asList(extraterrestrialDirectNormalRadiation));
        dataframe.addColumn(new Measure("horizontalInfraredRadiationFromSky", horizontalInfraredRadiationFromSky.getClass().getComponentType()), Arrays.asList(horizontalInfraredRadiationFromSky));
        dataframe.addColumn(new Measure("globalHorizontalRadiation", globalHorizontalRadiation.getClass().getComponentType()), Arrays.asList(globalHorizontalRadiation));
        dataframe.addColumn(new Measure("directNormalRadiation", directNormalRadiation.getClass().getComponentType()), Arrays.asList(directNormalRadiation));
        dataframe.addColumn(new Measure("diffuseHorizontalRadiation", diffuseHorizontalRadiation.getClass().getComponentType()), Arrays.asList(diffuseHorizontalRadiation));
        dataframe.addColumn(new Measure("globalHorizontalIlluminance", globalHorizontalIlluminance.getClass().getComponentType()), Arrays.asList(globalHorizontalIlluminance));
        dataframe.addColumn(new Measure("directNormalIlluminance", directNormalIlluminance.getClass().getComponentType()), Arrays.asList(directNormalIlluminance));
        dataframe.addColumn(new Measure("diffuseHorizontalIlluminance", diffuseHorizontalIlluminance.getClass().getComponentType()), Arrays.asList(diffuseHorizontalIlluminance));
        dataframe.addColumn(new Measure("zenithLuminance", zenithLuminance.getClass().getComponentType()), Arrays.asList(zenithLuminance));
        dataframe.addColumn(new Measure("windDirection", windDirection.getClass().getComponentType()), Arrays.asList(windDirection));
        dataframe.addColumn(new Measure("windSpeed", windSpeed.getClass().getComponentType()), Arrays.asList(windSpeed));
        dataframe.addColumn(new Measure("totalSkyCover", totalSkyCover.getClass().getComponentType()), Arrays.asList(totalSkyCover));
        dataframe.addColumn(new Measure("opaqueSkyCover", opaqueSkyCover.getClass().getComponentType()), Arrays.asList(opaqueSkyCover));
        dataframe.addColumn(new Measure("visibility", visibility.getClass().getComponentType()), Arrays.asList(visibility));
        dataframe.addColumn(new Measure("ceilingHeight", ceilingHeight.getClass().getComponentType()), Arrays.asList(ceilingHeight));
        dataframe.addColumn(new Measure("presentWeatherObservation", presentWeatherObservation.getClass().getComponentType()), Arrays.asList(presentWeatherObservation));
        dataframe.addColumn(new Measure("presentWeatherCodes", presentWeatherCodes.getClass().getComponentType()), Arrays.asList(presentWeatherCodes));
        dataframe.addColumn(new Measure("precipitableWater", precipitableWater.getClass().getComponentType()), Arrays.asList(precipitableWater));
        dataframe.addColumn(new Measure("aerosolOpticalDepth", aerosolOpticalDepth.getClass().getComponentType()), Arrays.asList(aerosolOpticalDepth));
        dataframe.addColumn(new Measure("snowDepth", snowDepth.getClass().getComponentType()), Arrays.asList(snowDepth));
        dataframe.addColumn(new Measure("daysSinceLastSnowfall", daysSinceLastSnowfall.getClass().getComponentType()), Arrays.asList(daysSinceLastSnowfall));
        dataframe.addColumn(new Measure("albedo", albedo.getClass().getComponentType()), Arrays.asList(albedo));
        dataframe.addColumn(new Measure("liquidPrecipitationDepth", liquidPrecipitationDepth.getClass().getComponentType()), Arrays.asList(liquidPrecipitationDepth));
        dataframe.addColumn(new Measure("liquidPrecipitationQuantity", liquidPrecipitationQuantity.getClass().getComponentType()), Arrays.asList(liquidPrecipitationQuantity));

        DateTimeFormatter formatter
                = DateTimeFormatter.ofPattern("yyyy,MM,dd,");
        String actual = dataframe.stream().map(row -> {
            LocalDateTime time = LocalDateTime.ofInstant(row.getDatetime(), ZoneOffset.UTC);
            String line = row.stream().map(Object::toString).collect(Collectors.joining(","));
            return time.format(formatter)
                    + String.format("%02d", time.getHour() + 1)
                    + ",60,"
                    + line;
        }).collect(Collectors.joining("\n"));

        assertEquals(expected, actual);
    }

    @Test
    public void exportTest() {
        Epw epw = new Epw();
        final String expected
                = "LOCATION,Boulder,CO,United States,TMY2 94018,724699,40.02,-105.25,-7.0,1634.0\n"
                + "DESIGN CONDITIONS,header line 2 (design conditions)\n"
                + "TYPICAL/EXTREME PERIODS,2,First period,Typical,2000-03-10,2000-12-12,Second period,Extreme,2005-07-16,2005-11-02\n"
                + "GROUND TEMPERATURES,3,0.5,,,,3.98,1.38,0.68,1.29,4.79,8.71,12.4,15.07,15.85,14.59,11.57,7.76,2.0,,,,6.31,3.88,2.72,2.68,4.53,7.21,10.09,12.57,13.84,13.6,11.9,9.28,4.0,,,,7.83,5.94,4.78,4.43,5.11,6.67,8.61,10.51,11.79,12.1,11.37,9.84\n"
                + "HOLIDAYS/DAYLIGHT SAVINGS,No,0,0,2,Easter,2016-03-27,Christmas,2016-12-25\n"
                + "COMMENTS 1,Boulder CO weather data taken from TMY2 data\n"
                + "COMMENTS 2,\n"
                + "DATA PERIODS,2,1,First period,Friday,2000-03-10,2000-12-12,1,Second period,Saturday,2005-07-16,2005-11-02\n"
                + "1970,01,01,01,60,B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7,-7.0,-8.5,88,83400,0,0,9999,0,0,0,0,0,0,0,230,1.5,10,8,9999.0,99999,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,02,60,A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7,-7.2,-8.3,91,83400,0,0,9999,0,0,0,0,0,0,0,220,1.5,10,8,11.3,1128,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,03,60,B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7,-8.1,-8.8,94,83400,0,0,9999,0,0,0,0,0,0,0,210,1.5,7,6,9999.0,99999,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,04,60,B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7,-9.1,-9.4,97,83400,0,0,9999,0,0,0,0,0,0,0,210,1.5,5,4,9999.0,99999,0,999999999,3,0.034,3,0,0.0,0.0,0.0\n"
                + "1970,01,01,05,60,A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7,-10.0,-10.0,100,83300,0,0,9999,0,0,0,0,0,0,0,200,1.5,2,2,24.1,77777,0,999999999,3,0.034,3,0,0.0,0.0,0.0";

        Instant[] datetimes = {
            LocalDateTime.of(1970, Month.JANUARY, 1, 0, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 1, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 2, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 3, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(1970, Month.JANUARY, 1, 4, 0).toInstant(ZoneOffset.UTC)};

        String[] dataSourceAndUncertaintyFlags = {
            "B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7",
            "A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7",
            "B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7",
            "B8E7B8B8?9?0?0?0?0?0?0B8B8B8B8?0?0F8F8A7E7",
            "A7A7A7A7?9?0?0?0?0?0?0A7A7A7A7A7A7F8F8A7E7"};

        Double[] dryBulbTemp = {-7.0, -7.2, -8.1, -9.1, -10.0};
        Double[] dewPointTemp = {-8.5, -8.3, -8.8, -9.4, -10.0};
        Integer[] relativeHumidity = {88, 91, 94, 97, 100};
        Integer[] atmosphericStationPressure = {83400, 83400, 83400, 83400, 83300};
        Integer[] extraterrestrialHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] extraterrestrialDirectNormalRadiation = {0, 0, 0, 0, 0};
        Integer[] horizontalInfraredRadiationFromSky = {9999, 9999, 9999, 9999, 9999};
        Integer[] globalHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] directNormalRadiation = {0, 0, 0, 0, 0};
        Integer[] diffuseHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] globalHorizontalIlluminance = {0, 0, 0, 0, 0};
        Integer[] directNormalIlluminance = {0, 0, 0, 0, 0};
        Integer[] diffuseHorizontalIlluminance = {0, 0, 0, 0, 0};
        Integer[] zenithLuminance = {0, 0, 0, 0, 0};
        Integer[] windDirection = {230, 220, 210, 210, 200};
        Double[] windSpeed = {1.5, 1.5, 1.5, 1.5, 1.5};
        Integer[] totalSkyCover = {10, 10, 7, 5, 2};
        Integer[] opaqueSkyCover = {8, 8, 6, 4, 2};
        Double[] visibility = {9999.0, 11.3, 9999.0, 9999.0, 24.1};
        Integer[] ceilingHeight = {99999, 1128, 99999, 99999, 77777};
        Integer[] presentWeatherObservation = {0, 0, 0, 0, 0};
        Integer[] presentWeatherCodes = {999999999, 999999999, 999999999, 999999999, 999999999};
        Integer[] precipitableWater = {3, 3, 3, 3, 3};
        Double[] aerosolOpticalDepth = {0.034, 0.034, 0.034, 0.034, 0.034};
        Integer[] snowDepth = {3, 3, 3, 3, 3};
        Integer[] daysSinceLastSnowfall = {0, 0, 0, 0, 0};
        Double[] albedo = {0.0, 0.0, 0.0, 0.0, 0.0};
        Double[] liquidPrecipitationDepth = {0.0, 0.0, 0.0, 0.0, 0.0};
        Double[] liquidPrecipitationQuantity = {0.0, 0.0, 0.0, 0.0, 0.0};

        DataFrame dataframe = new DataFrame(Arrays.asList(datetimes));

        dataframe.addColumn(new Measure("dataSourceAndUncertaintyFlags", String.class), Arrays.asList(dataSourceAndUncertaintyFlags));

        dataframe.addColumn(new Measure("dryBulbTemp", dryBulbTemp.getClass().getComponentType()), Arrays.asList(dryBulbTemp));
        dataframe.addColumn(new Measure("dewPointTemp", dewPointTemp.getClass().getComponentType()), Arrays.asList(dewPointTemp));
        dataframe.addColumn(new Measure("relativeHumidity", relativeHumidity.getClass().getComponentType()), Arrays.asList(relativeHumidity));
        dataframe.addColumn(new Measure("atmosphericStationPressure", atmosphericStationPressure.getClass().getComponentType()), Arrays.asList(atmosphericStationPressure));
        dataframe.addColumn(new Measure("extraterrestrialHorizontalRadiation", extraterrestrialHorizontalRadiation.getClass().getComponentType()), Arrays.asList(extraterrestrialHorizontalRadiation));
        dataframe.addColumn(new Measure("extraterrestrialDirectNormalRadiation", extraterrestrialDirectNormalRadiation.getClass().getComponentType()), Arrays.asList(extraterrestrialDirectNormalRadiation));
        dataframe.addColumn(new Measure("horizontalInfraredRadiationFromSky", horizontalInfraredRadiationFromSky.getClass().getComponentType()), Arrays.asList(horizontalInfraredRadiationFromSky));
        dataframe.addColumn(new Measure("globalHorizontalRadiation", globalHorizontalRadiation.getClass().getComponentType()), Arrays.asList(globalHorizontalRadiation));
        dataframe.addColumn(new Measure("directNormalRadiation", directNormalRadiation.getClass().getComponentType()), Arrays.asList(directNormalRadiation));
        dataframe.addColumn(new Measure("diffuseHorizontalRadiation", diffuseHorizontalRadiation.getClass().getComponentType()), Arrays.asList(diffuseHorizontalRadiation));
        dataframe.addColumn(new Measure("globalHorizontalIlluminance", globalHorizontalIlluminance.getClass().getComponentType()), Arrays.asList(globalHorizontalIlluminance));
        dataframe.addColumn(new Measure("directNormalIlluminance", directNormalIlluminance.getClass().getComponentType()), Arrays.asList(directNormalIlluminance));
        dataframe.addColumn(new Measure("diffuseHorizontalIlluminance", diffuseHorizontalIlluminance.getClass().getComponentType()), Arrays.asList(diffuseHorizontalIlluminance));
        dataframe.addColumn(new Measure("zenithLuminance", zenithLuminance.getClass().getComponentType()), Arrays.asList(zenithLuminance));
        dataframe.addColumn(new Measure("windDirection", windDirection.getClass().getComponentType()), Arrays.asList(windDirection));
        dataframe.addColumn(new Measure("windSpeed", windSpeed.getClass().getComponentType()), Arrays.asList(windSpeed));
        dataframe.addColumn(new Measure("totalSkyCover", totalSkyCover.getClass().getComponentType()), Arrays.asList(totalSkyCover));
        dataframe.addColumn(new Measure("opaqueSkyCover", opaqueSkyCover.getClass().getComponentType()), Arrays.asList(opaqueSkyCover));
        dataframe.addColumn(new Measure("visibility", visibility.getClass().getComponentType()), Arrays.asList(visibility));
        dataframe.addColumn(new Measure("ceilingHeight", ceilingHeight.getClass().getComponentType()), Arrays.asList(ceilingHeight));
        dataframe.addColumn(new Measure("presentWeatherObservation", presentWeatherObservation.getClass().getComponentType()), Arrays.asList(presentWeatherObservation));
        dataframe.addColumn(new Measure("presentWeatherCodes", presentWeatherCodes.getClass().getComponentType()), Arrays.asList(presentWeatherCodes));
        dataframe.addColumn(new Measure("precipitableWater", precipitableWater.getClass().getComponentType()), Arrays.asList(precipitableWater));
        dataframe.addColumn(new Measure("aerosolOpticalDepth", aerosolOpticalDepth.getClass().getComponentType()), Arrays.asList(aerosolOpticalDepth));
        dataframe.addColumn(new Measure("snowDepth", snowDepth.getClass().getComponentType()), Arrays.asList(snowDepth));
        dataframe.addColumn(new Measure("daysSinceLastSnowfall", daysSinceLastSnowfall.getClass().getComponentType()), Arrays.asList(daysSinceLastSnowfall));
        dataframe.addColumn(new Measure("albedo", albedo.getClass().getComponentType()), Arrays.asList(albedo));
        dataframe.addColumn(new Measure("liquidPrecipitationDepth", liquidPrecipitationDepth.getClass().getComponentType()), Arrays.asList(liquidPrecipitationDepth));
        dataframe.addColumn(new Measure("liquidPrecipitationQuantity", liquidPrecipitationQuantity.getClass().getComponentType()), Arrays.asList(liquidPrecipitationQuantity));

        epw.setDataframe(dataframe);

        TypicalOrExtremePeriod firstTypicalOrExtremePeriod = new TypicalOrExtremePeriod("First period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(2000, Month.MARCH, 10), LocalDate.of(2000, Month.DECEMBER, 12));
        TypicalOrExtremePeriod secondTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Second period", TypicalOrExtremePeriod.Type.EXTREME, LocalDate.of(2005, Month.JULY, 16), LocalDate.of(2005, Month.NOVEMBER, 2));
        epw.setTypicalOrExtremePeriods(Arrays.asList(firstTypicalOrExtremePeriod, secondTypicalOrExtremePeriod));

        GroundTemperature firstTemperature = new GroundTemperature(.5, Double.NaN, Double.NaN, Double.NaN, 3.98, 1.38, 0.68, 1.29, 4.79, 8.71, 12.40, 15.07, 15.85, 14.59, 11.57, 7.76);
        GroundTemperature secondTemperature = new GroundTemperature(2, Double.NaN, Double.NaN, Double.NaN, 6.31, 3.88, 2.72, 2.68, 4.53, 7.21, 10.09, 12.57, 13.84, 13.60, 11.90, 9.28);
        GroundTemperature thirdTemperature = new GroundTemperature(4, Double.NaN, Double.NaN, Double.NaN, 7.83, 5.94, 4.78, 4.43, 5.11, 6.67, 8.61, 10.51, 11.79, 12.10, 11.37, 9.84);
        epw.setGroundTemperatures(Arrays.asList(firstTemperature, secondTemperature, thirdTemperature));

        epw.setDesignConditions(new DesignConditions("header line 2 (design conditions)"));

        DataPeriod firstDataPeriod = new DataPeriod(1, "First period", DayOfWeek.FRIDAY, LocalDate.of(2000, Month.MARCH, 10), LocalDate.of(2000, Month.DECEMBER, 12));
        DataPeriod secondDataPeriod = new DataPeriod(1, "Second period", DayOfWeek.SATURDAY, LocalDate.of(2005, Month.JULY, 16), LocalDate.of(2005, Month.NOVEMBER, 2));
        epw.setDataPeriods(Arrays.asList(firstDataPeriod, secondDataPeriod));

        epw.setLocation(new Location("Boulder", "CO", "United States", "TMY2 94018", "724699", 40.02, -105.25, -7.0, 1634));

        epw.setHolidaysOrDaylightSavings(new HolidaysOrDaylightSavings(HolidaysOrDaylightSavings.Observed.NO, 0, 0, 2, Arrays.asList(new HolidaysOrDaylightSavings.Holiday("Easter", LocalDate.of(2016, Month.MARCH, 27)), new HolidaysOrDaylightSavings.Holiday("Christmas", LocalDate.of(2016, Month.DECEMBER, 25)))));

        Comments firstComment = new Comments(1, "Boulder CO weather data taken from TMY2 data");
        Comments secondComment = new Comments(2, "");
        epw.setComments(Arrays.asList(firstComment, secondComment));

        epw.setZone(ZoneOffset.UTC);

        final String actual = epw.export();

        assertEquals(expected, actual);
    }
}
