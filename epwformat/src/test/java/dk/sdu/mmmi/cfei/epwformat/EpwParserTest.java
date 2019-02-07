package dk.sdu.mmmi.cfei.epwformat;

import dk.sdu.mmmi.cfei.dataframes.DataFrame;
import dk.sdu.mmmi.cfei.dataframes.Measure;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cgim
 */
public class EpwParserTest {

    @Test
    public void locationTest() {
        EpwParser parser = new EpwParser();
        Location expected = new Location("COPENHAGEN", "-", "DNK", "IWEC Data", "061800", 55.63, 12.67, 1.0, 5.0);
        String line = "LOCATION,COPENHAGEN,-,DNK,IWEC Data,061800,55.63,12.67,1.0,5.0";
        Optional<Location> actual = parser.parseLocation(line);
        assertEquals(Optional.of(expected), actual);
    }

    @Test
    public void dataPeriodsTest() {
        EpwParser parser = new EpwParser();
        DataPeriod firstPeriod = new DataPeriod(1, "First period", DayOfWeek.FRIDAY, LocalDate.of(2000, Month.MARCH, 10), LocalDate.of(2000, Month.DECEMBER, 12));
        DataPeriod secondPeriod = new DataPeriod(1, "Second period", DayOfWeek.SATURDAY, LocalDate.of(2005, Month.JULY, 16), LocalDate.of(2005, Month.NOVEMBER, 2));
        List<DataPeriod> expected = Arrays.asList(firstPeriod, secondPeriod);
        String line = "DATA PERIODS,2,1,First period,Friday,2000-03-10,2000-12-12,1,Second period,Saturday,2005-07-16,2005-11-02";
        List<DataPeriod> actual = parser.parseDataPeriods(line);
        assertEquals(expected, actual);
    }

    @Test
    public void typicalOrExtremePeriodsTest() {
        EpwParser parser = new EpwParser();
        TypicalOrExtremePeriod firstPeriod = new TypicalOrExtremePeriod("First period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(2000, Month.MARCH, 10), LocalDate.of(2000, Month.DECEMBER, 12));
        TypicalOrExtremePeriod secondPeriod = new TypicalOrExtremePeriod("Second period", TypicalOrExtremePeriod.Type.EXTREME, LocalDate.of(2005, Month.JULY, 16), LocalDate.of(2005, Month.NOVEMBER, 2));
        List<TypicalOrExtremePeriod> expected = Arrays.asList(firstPeriod, secondPeriod);
        String line = "TYPICAL/EXTREME PERIODS,2,First period,Typical,2000-03-10,2000-12-12,Second period,Extreme,2005-07-16,2005-11-02";
        List<TypicalOrExtremePeriod> actual = parser.parseTypicalOrExtremePeriods(line);
        assertEquals(expected, actual);
    }

    @Test
    public void typicalOrExtremePeriodsWithOtherDateFormatTest() {
        final int YEAR = 2015;
        EpwParser parser = new EpwParser(YEAR);
        TypicalOrExtremePeriod firstPeriod = new TypicalOrExtremePeriod("First period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(YEAR, Month.MARCH, 10), LocalDate.of(YEAR, Month.DECEMBER, 12));
        TypicalOrExtremePeriod secondPeriod = new TypicalOrExtremePeriod("Second period", TypicalOrExtremePeriod.Type.EXTREME, LocalDate.of(2005, Month.JULY, 16), LocalDate.of(2005, Month.NOVEMBER, 2));
        List<TypicalOrExtremePeriod> expected = Arrays.asList(firstPeriod, secondPeriod);
        String line = "TYPICAL/EXTREME PERIODS,2,First period,Typical, 3/10,12/12,Second period,Extreme,2005-07-16,2005-11-02";
        List<TypicalOrExtremePeriod> actual = parser.parseTypicalOrExtremePeriods(line);
        assertEquals(expected, actual);
    }

    @Test
    public void groundTemperaturesTest() {
        EpwParser parser = new EpwParser();
        GroundTemperature firstTemperature = new GroundTemperature(.5, Double.NaN, Double.NaN, Double.NaN, 3.98, 1.38, 0.68, 1.29, 4.79, 8.71, 12.40, 15.07, 15.85, 14.59, 11.57, 7.76);
        GroundTemperature secondTemperature = new GroundTemperature(2, Double.NaN, Double.NaN, Double.NaN, 6.31, 3.88, 2.72, 2.68, 4.53, 7.21, 10.09, 12.57, 13.84, 13.60, 11.90, 9.28);
        GroundTemperature thirdTemperature = new GroundTemperature(4, Double.NaN, Double.NaN, Double.NaN, 7.83, 5.94, 4.78, 4.43, 5.11, 6.67, 8.61, 10.51, 11.79, 12.10, 11.37, 9.84);
        List<GroundTemperature> expected = Arrays.asList(firstTemperature, secondTemperature, thirdTemperature);
        String line = "GROUND TEMPERATURES,3,.5,,,,3.98,1.38,0.68,1.29,4.79,8.71,12.40,15.07,15.85,14.59,11.57,7.76,2,,,,6.31,3.88,2.72,2.68,4.53,7.21,10.09,12.57,13.84,13.60,11.90,9.28,4,,,,7.83,5.94,4.78,4.43,5.11,6.67,8.61,10.51,11.79,12.10,11.37,9.84";
        List<GroundTemperature> actual = parser.parseGroundTemperature(line);
        assertEquals(expected, actual);
    }

    @Test
    public void commentsTest() {
        EpwParser parser = new EpwParser();
        Comments expected = new Comments(1, "Some comments");
        String line = "COMMENTS 1,Some comments";
        Optional<Comments> actual = parser.parseComment(line);
        assertEquals(Optional.of(expected), actual);
    }

    @Test
    public void secondCommentsTest() {
        EpwParser parser = new EpwParser();
        Comments expected = new Comments(1, "\"IWEC- WMO#061800 - Europe -- Original Source Data (c) 2001 American Society of Heating, Refrigerating and Air-Conditioning Engineers (ASHRAE), Inc., Atlanta, GA, USA.  www.ashrae.org  All rights reserved as noted in the License Agreement and Additional Conditions. DISCLAIMER OF WARRANTIES: The data is provided 'as is' without warranty of any kind, either expressed or implied. The entire risk as to the quality and performance of the data is with you. In no event will ASHRAE or its contractors be liable to you for any damages, including without limitation any lost profits, lost savings, or other incidental or consequential damages arising out of the use or inability to use this data.\"");
        String line = "COMMENTS 1,\"IWEC- WMO#061800 - Europe -- Original Source Data (c) 2001 American Society of Heating, Refrigerating and Air-Conditioning Engineers (ASHRAE), Inc., Atlanta, GA, USA.  www.ashrae.org  All rights reserved as noted in the License Agreement and Additional Conditions. DISCLAIMER OF WARRANTIES: The data is provided 'as is' without warranty of any kind, either expressed or implied. The entire risk as to the quality and performance of the data is with you. In no event will ASHRAE or its contractors be liable to you for any damages, including without limitation any lost profits, lost savings, or other incidental or consequential damages arising out of the use or inability to use this data.\"";
        Optional<Comments> actual = parser.parseComment(line);
        assertEquals(Optional.of(expected), actual);
    }

    @Test
    public void designConditionsTest() {
        EpwParser parser = new EpwParser();
        DesignConditions expected = new DesignConditions("header line 2 (design conditions)");
        String line = "DESIGN CONDITIONS,header line 2 (design conditions)";
        Optional<DesignConditions> actual = parser.parseDesignConditions(line);
        assertEquals(Optional.of(expected), actual);
    }

    @Test
    public void holidaysOrDaylightSavingsTest() {
        EpwParser parser = new EpwParser();
        HolidaysOrDaylightSavings expected = new HolidaysOrDaylightSavings(HolidaysOrDaylightSavings.Observed.NO, 0, 0, 2, Arrays.asList(new HolidaysOrDaylightSavings.Holiday("Easter", LocalDate.of(2016, Month.MARCH, 27)), new HolidaysOrDaylightSavings.Holiday("Christmas", LocalDate.of(2016, Month.DECEMBER, 25))));
        String line = "HOLIDAYS/DAYLIGHT SAVINGS,No,0,0,2,Easter,2016-03-27,Christmas,2016-12-25";
        Optional<HolidaysOrDaylightSavings> actual = parser.parseHolidaysOrDaylightSavings(line);
        assertEquals(Optional.of(expected), actual);
    }

    @Test
    public void parserTest() {
        final int YEAR = 2015;
        EpwParser parser = new EpwParser(YEAR);
        String content = "LOCATION,COPENHAGEN,-,DNK,IWEC Data,061800,55.63,12.67,1.0,5.0\n"
                + "DESIGN CONDITIONS,1,Climate Design Data 2009 ASHRAE Handbook,,Heating,2,-9.2,-6.7,-12,1.3,-7.6,-10,1.6,-4.9,14.7,4.1,13.3,3.4,4.8,50,Cooling,7,8,25.5,17.9,24,17.3,22.2,16.5,19.3,23.4,18.4,22.3,17.5,21.2,4.6,160,17.9,12.9,20.8,16.9,12.1,20,15.9,11.3,19.3,54.8,23.1,51.8,22.1,49.2,21.1,1044,Extremes,12.7,11.4,10.3,22.4,-11,27.9,3.6,1.7,-13.6,29.1,-15.6,30,-17.6,31,-20.2,32.2\n"
                + "TYPICAL/EXTREME PERIODS,6,Summer - Week Nearest Max Temperature For Period,Extreme,8/ 3,8/ 9,Summer - Week Nearest Average Temperature For Period,Typical,7/ 6,7/12,Winter - Week Nearest Min Temperature For Period,Extreme,2/10,2/16,Winter - Week Nearest Average Temperature For Period,Typical,12/15,12/21,Autumn - Week Nearest Average Temperature For Period,Typical,9/22,9/28,Spring - Week Nearest Average Temperature For Period,Typical,4/ 5,4/11\n"
                + "GROUND TEMPERATURES,3,.5,,,,3.98,1.38,0.68,1.29,4.79,8.71,12.40,15.07,15.85,14.59,11.57,7.76,2,,,,6.31,3.88,2.72,2.68,4.53,7.21,10.09,12.57,13.84,13.60,11.90,9.28,4,,,,7.83,5.94,4.78,4.43,5.11,6.67,8.61,10.51,11.79,12.10,11.37,9.84\n"
                + "HOLIDAYS/DAYLIGHT SAVINGS,No,0,0,0\n"
                + "COMMENTS 1,\"IWEC- WMO#061800 - Europe -- Original Source Data (c) 2001 American Society of Heating, Refrigerating and Air-Conditioning Engineers (ASHRAE), Inc., Atlanta, GA, USA.  www.ashrae.org  All rights reserved as noted in the License Agreement and Additional Conditions. DISCLAIMER OF WARRANTIES: The data is provided 'as is' without warranty of any kind, either expressed or implied. The entire risk as to the quality and performance of the data is with you. In no event will ASHRAE or its contractors be liable to you for any damages, including without limitation any lost profits, lost savings, or other incidental or consequential damages arising out of the use or inability to use this data.\"\n"
                + "COMMENTS 2, -- Ground temps produced with a standard soil diffusivity of 2.3225760E-03 {m**2/day}\n"
                + "DATA PERIODS,1,1,Data,Sunday, 1/ 1,12/31\n"
                + "1984,1,1,1,60,C9C9C9C9*0?9?9?9?9?9?9?9A7A7A7A7A7A7*0E8*0*0,7.0,4.6,85,99500,0,1415,322,0,0,0,0,0,0,0,250,11.3,10,10,8.0,360,0,999999099,0,0.0680,0,88,0.000,0.0,0.0\n"
                + "1984,1,1,2,60,C9C9C9C9*0?9?9?9?9?9?9?9*0B8B8B8B8*0*0E8*0*0,7.1,4.5,84,99400,0,1415,323,0,0,0,0,0,0,0,250,12.7,10,10,8.7,360,9,999999999,0,0.0680,0,88,0.000,0.0,0.0\n"
                + "1984,1,1,3,60,C9C9C9C9*0?9?9?9?9?9?9?9*0B8B8B8B8*0*0E8*0*0,7.2,4.5,83,99300,0,1415,323,0,0,0,0,0,0,0,250,14.0,10,10,9.3,360,9,999999999,0,0.0680,0,88,0.000,0.0,0.0\n"
                + "1984,1,1,4,60,C9C9C9C9*0?9?9?9?9?9?9?9A7A7A7A7A7A7*0E8*0*0,7.4,4.5,82,99200,0,1415,324,0,0,0,0,0,0,0,260,15.4,10,10,10.0,480,0,909999999,0,0.0680,0,88,0.000,0.0,0.0\n"
                + "1984,1,1,5,60,C9C9C9C9*0?9?9?9?9?9?9?9*0B8B8B8B8*0*0E8*0*0,7.6,4.5,81,99200,0,1415,325,0,0,0,0,0,0,0,260,15.4,10,10,10.7,480,9,999999999,0,0.0680,0,88,0.000,0.0,0.0";

        Epw actual = parser.parse(content);

        DesignConditions expectedDesignConditions = new DesignConditions("1,Climate Design Data 2009 ASHRAE Handbook,,Heating,2,-9.2,-6.7,-12,1.3,-7.6,-10,1.6,-4.9,14.7,4.1,13.3,3.4,4.8,50,Cooling,7,8,25.5,17.9,24,17.3,22.2,16.5,19.3,23.4,18.4,22.3,17.5,21.2,4.6,160,17.9,12.9,20.8,16.9,12.1,20,15.9,11.3,19.3,54.8,23.1,51.8,22.1,49.2,21.1,1044,Extremes,12.7,11.4,10.3,22.4,-11,27.9,3.6,1.7,-13.6,29.1,-15.6,30,-17.6,31,-20.2,32.2");

        Location expectedLocation = new Location("COPENHAGEN", "-", "DNK", "IWEC Data", "061800", 55.63, 12.67, 1.0, 5.0);

        DataPeriod firstDataPeriod = new DataPeriod(1, "Data", DayOfWeek.SUNDAY, LocalDate.of(YEAR, Month.JANUARY, 1), LocalDate.of(YEAR, Month.DECEMBER, 31));
        List<DataPeriod> expectedDataPeriods = Arrays.asList(firstDataPeriod);

        TypicalOrExtremePeriod firstTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Summer - Week Nearest Max Temperature For Period", TypicalOrExtremePeriod.Type.EXTREME, LocalDate.of(YEAR, Month.AUGUST, 3), LocalDate.of(YEAR, Month.AUGUST, 9));
        TypicalOrExtremePeriod secondTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Summer - Week Nearest Average Temperature For Period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(YEAR, Month.JULY, 6), LocalDate.of(YEAR, Month.JULY, 12));
        TypicalOrExtremePeriod thirdTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Winter - Week Nearest Min Temperature For Period", TypicalOrExtremePeriod.Type.EXTREME, LocalDate.of(YEAR, Month.FEBRUARY, 10), LocalDate.of(YEAR, Month.FEBRUARY, 16));
        TypicalOrExtremePeriod fourthTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Winter - Week Nearest Average Temperature For Period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(YEAR, Month.DECEMBER, 15), LocalDate.of(YEAR, Month.DECEMBER, 21));
        TypicalOrExtremePeriod fifthTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Autumn - Week Nearest Average Temperature For Period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(YEAR, Month.SEPTEMBER, 22), LocalDate.of(YEAR, Month.SEPTEMBER, 28));
        TypicalOrExtremePeriod sixthTypicalOrExtremePeriod = new TypicalOrExtremePeriod("Spring - Week Nearest Average Temperature For Period", TypicalOrExtremePeriod.Type.TYPICAL, LocalDate.of(YEAR, Month.APRIL, 5), LocalDate.of(YEAR, Month.APRIL, 11));
        List<TypicalOrExtremePeriod> expectedTypicalOrExtremePeriods = Arrays.asList(firstTypicalOrExtremePeriod, secondTypicalOrExtremePeriod, thirdTypicalOrExtremePeriod, fourthTypicalOrExtremePeriod, fifthTypicalOrExtremePeriod, sixthTypicalOrExtremePeriod);

        HolidaysOrDaylightSavings expectedHolidaysOrDaylightSavings = new HolidaysOrDaylightSavings(HolidaysOrDaylightSavings.Observed.NO, 0, 0, 0, Arrays.asList());

        GroundTemperature firstGroundTemperature = new GroundTemperature(.5, Double.NaN, Double.NaN, Double.NaN, 3.98, 1.38, 0.68, 1.29, 4.79, 8.71, 12.40, 15.07, 15.85, 14.59, 11.57, 7.76);
        GroundTemperature secondGroundTemperature = new GroundTemperature(2, Double.NaN, Double.NaN, Double.NaN, 6.31, 3.88, 2.72, 2.68, 4.53, 7.21, 10.09, 12.57, 13.84, 13.60, 11.90, 9.28);
        GroundTemperature thirdGroundTemperature = new GroundTemperature(4, Double.NaN, Double.NaN, Double.NaN, 7.83, 5.94, 4.78, 4.43, 5.11, 6.67, 8.61, 10.51, 11.79, 12.10, 11.37, 9.84);
        List<GroundTemperature> expectedGroundTemperatures = Arrays.asList(firstGroundTemperature, secondGroundTemperature, thirdGroundTemperature);

        Comments firstComment = new Comments(1, "\"IWEC- WMO#061800 - Europe -- Original Source Data (c) 2001 American Society of Heating, Refrigerating and Air-Conditioning Engineers (ASHRAE), Inc., Atlanta, GA, USA.  www.ashrae.org  All rights reserved as noted in the License Agreement and Additional Conditions. DISCLAIMER OF WARRANTIES: The data is provided 'as is' without warranty of any kind, either expressed or implied. The entire risk as to the quality and performance of the data is with you. In no event will ASHRAE or its contractors be liable to you for any damages, including without limitation any lost profits, lost savings, or other incidental or consequential damages arising out of the use or inability to use this data.\"");
        Comments secondComment = new Comments(2, " -- Ground temps produced with a standard soil diffusivity of 2.3225760E-03 {m**2/day}");

        Instant[] datetimes = {
            LocalDateTime.of(YEAR, Month.JANUARY, 1, 0, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(YEAR, Month.JANUARY, 1, 1, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(YEAR, Month.JANUARY, 1, 2, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(YEAR, Month.JANUARY, 1, 3, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(YEAR, Month.JANUARY, 1, 4, 0).toInstant(ZoneOffset.UTC)};

        String[] dataSourceAndUncertaintyFlags = {
            "C9C9C9C9*0?9?9?9?9?9?9?9A7A7A7A7A7A7*0E8*0*0",
            "C9C9C9C9*0?9?9?9?9?9?9?9*0B8B8B8B8*0*0E8*0*0",
            "C9C9C9C9*0?9?9?9?9?9?9?9*0B8B8B8B8*0*0E8*0*0",
            "C9C9C9C9*0?9?9?9?9?9?9?9A7A7A7A7A7A7*0E8*0*0",
            "C9C9C9C9*0?9?9?9?9?9?9?9*0B8B8B8B8*0*0E8*0*0"};

        Double[] dryBulbTemp = {7.0, 7.1, 7.2, 7.4, 7.6};
        Double[] dewPointTemp = {4.6, 4.5, 4.5, 4.5, 4.5};
        Integer[] relativeHumidity = {85, 84, 83, 82, 81};
        Integer[] atmosphericStationPressure = {99500, 99400, 99300, 99200, 99200};
        Integer[] extraterrestrialHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] extraterrestrialDirectNormalRadiation = {1415, 1415, 1415, 1415, 1415};
        Integer[] horizontalInfraredRadiationFromSky = {322, 323, 323, 324, 325};
        Integer[] globalHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] directNormalRadiation = {0, 0, 0, 0, 0};
        Integer[] diffuseHorizontalRadiation = {0, 0, 0, 0, 0};
        Integer[] globalHorizontalIlluminance = {0, 0, 0, 0, 0};
        Integer[] directNormalIlluminance = {0, 0, 0, 0, 0};
        Integer[] diffuseHorizontalIlluminance = {0, 0, 0, 0, 0};
        Integer[] zenithLuminance = {0, 0, 0, 0, 0};
        Integer[] windDirection = {250, 250, 250, 260, 260};
        Double[] windSpeed = {11.3, 12.7, 14.0, 15.4, 15.4};
        Integer[] totalSkyCover = {10, 10, 10, 10, 10};
        Integer[] opaqueSkyCover = {10, 10, 10, 10, 10};
        Double[] visibility = {8.0, 8.7, 9.3, 10.0, 10.7};
        Integer[] ceilingHeight = {360, 360, 360, 480, 480};
        Integer[] presentWeatherObservation = {0, 9, 9, 0, 9};
        Integer[] presentWeatherCodes = {999999099, 999999999, 999999999, 909999999, 999999999};
        Integer[] precipitableWater = {0, 0, 0, 0, 0};
        Double[] aerosolOpticalDepth = {0.0680, 0.0680, 0.0680, 0.0680, 0.0680};
        Integer[] snowDepth = {0, 0, 0, 0, 0};
        Integer[] daysSinceLastSnowfall = {88, 88, 88, 88, 88};
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

        Epw expected = new Epw();
        expected.setLocation(expectedLocation);
        expected.setDesignConditions(expectedDesignConditions);
        expected.setGroundTemperatures(expectedGroundTemperatures);
        expected.setHolidaysOrDaylightSavings(expectedHolidaysOrDaylightSavings);
        expected.setDataPeriods(expectedDataPeriods);
        expected.setTypicalOrExtremePeriods(expectedTypicalOrExtremePeriods);
        expected.setComments(Arrays.asList(firstComment, secondComment));
        expected.setDataframe(dataframe);

        assertEquals(expected.getLocation(), actual.getLocation());
        assertEquals(expected.getDesignConditions(), actual.getDesignConditions());
        assertEquals(expected.getTypicalOrExtremePeriods(), actual.getTypicalOrExtremePeriods());
        assertEquals(expected.getHolidaysOrDaylightSavings(), actual.getHolidaysOrDaylightSavings());
        assertEquals(expected.getDataPeriods(), actual.getDataPeriods());
        assertEquals(expected.getComments(), actual.getComments());
        assertEquals(expected.getGroundTemperatures(), actual.getGroundTemperatures());
        assertEquals(expected.getDataframe(), actual.getDataframe());
        assertEquals(expected, actual);
    }
}
