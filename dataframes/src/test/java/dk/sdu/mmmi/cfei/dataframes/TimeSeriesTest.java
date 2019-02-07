package dk.sdu.mmmi.cfei.dataframes;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cgim
 */
public class TimeSeriesTest {

    @Test
    public void resampleTest() {
        Instant[] datetimesOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        ArrayList<Instant> timestampsOriginal = new ArrayList<>(Arrays.asList(datetimesOriginalArray));
        ArrayList<Double> valuesOriginal = new ArrayList<>(Arrays.asList(firstOriginalArray));
        TimeSeries<Double> original = new TimeSeries<>(timestampsOriginal, valuesOriginal, Double.class);

        Instant[] timestampsExpectedArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstExpectedArray = {1., 3., 5., 7.};
        ArrayList<Instant> timestampsExpected = new ArrayList<>(Arrays.asList(timestampsExpectedArray));
        ArrayList<Double> firstExpected = new ArrayList<>(Arrays.asList(firstExpectedArray));
        TimeSeries<Double> expected = new TimeSeries<>(timestampsExpected, firstExpected, Double.class);

        TimeSeries<Number> resampled = original.resample(Duration.ofMinutes(20));

        assertEquals(expected, resampled);
    }

    @Test
    public void setTest() {
        Instant[] firstTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] firstValuesArray = {24.6, 28.7, 26.3, 13.8, 27.4};
        ArrayList<Instant> firstTimestamps = new ArrayList<>(Arrays.asList(firstTimestampsArray));
        ArrayList<Double> firstValues = new ArrayList<>(Arrays.asList(firstValuesArray));
        TimeSeries<Double> first = new TimeSeries<>(firstTimestamps, firstValues, Double.class);

        Instant[] secondTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] secondValuesArray = {24.6, 28.7, 555.5, 13.8, 27.4};
        ArrayList<Instant> secondTimestamps = new ArrayList<>(Arrays.asList(secondTimestampsArray));
        ArrayList<Double> secondValues = new ArrayList<>(Arrays.asList(secondValuesArray));
        TimeSeries<Double> second = new TimeSeries<>(secondTimestamps, secondValues, Double.class);

        first.set(
                LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
                555.5);

        assertEquals(second, first);
    }

    @Test
    public void setContiguousTimeseriesTest() {
        Instant[] firstTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] firstValuesArray = {24.6, 28.7, 26.3, 13.8, 27.4};
        ArrayList<Instant> firstTimestamps = new ArrayList<>(Arrays.asList(firstTimestampsArray));
        ArrayList<Double> firstValues = new ArrayList<>(Arrays.asList(firstValuesArray));
        TimeSeries<Double> first = new TimeSeries<>(firstTimestamps, firstValues, Double.class);

        Instant[] secondTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC)};
        Double[] secondValuesArray = {999.0, 666.6};
        ArrayList<Instant> secondTimestamps = new ArrayList<>(Arrays.asList(secondTimestampsArray));
        ArrayList<Double> secondValues = new ArrayList<>(Arrays.asList(secondValuesArray));
        TimeSeries<Double> second = new TimeSeries<>(secondTimestamps, secondValues, Double.class);

        Instant[] thirdTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] thirdValuesArray = {24.6, 28.7, 999.0, 666.6, 27.4};
        ArrayList<Instant> thirdTimestamps = new ArrayList<>(Arrays.asList(thirdTimestampsArray));
        ArrayList<Double> thirdValues = new ArrayList<>(Arrays.asList(thirdValuesArray));
        TimeSeries<Double> third = new TimeSeries<>(thirdTimestamps, thirdValues, Double.class);

        first.setContiguous(second);

        assertEquals(third, first);
    }

    @Test
    public void setTimeseriesTest() {
        Instant[] firstTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] firstValuesArray = {24.6, 28.7, 26.3, 13.8, 27.4};
        ArrayList<Instant> firstTimestamps = new ArrayList<>(Arrays.asList(firstTimestampsArray));
        ArrayList<Double> firstValues = new ArrayList<>(Arrays.asList(firstValuesArray));
        TimeSeries<Double> first = new TimeSeries<>(firstTimestamps, firstValues, Double.class);

        Instant[] secondTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] secondValuesArray = {999.0, 666.6};
        ArrayList<Instant> secondTimestamps = new ArrayList<>(Arrays.asList(secondTimestampsArray));
        ArrayList<Double> secondValues = new ArrayList<>(Arrays.asList(secondValuesArray));
        TimeSeries<Double> second = new TimeSeries<>(secondTimestamps, secondValues, Double.class);

        Instant[] thirdTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] thirdValuesArray = {24.6, 28.7, 999.0, 13.8, 666.6};
        ArrayList<Instant> thirdTimestamps = new ArrayList<>(Arrays.asList(thirdTimestampsArray));
        ArrayList<Double> thirdValues = new ArrayList<>(Arrays.asList(thirdValuesArray));
        TimeSeries<Double> third = new TimeSeries<>(thirdTimestamps, thirdValues, Double.class);

        first.set(second, false);

        assertEquals(third, first);
    }

    @Test
    public void setTimeseriesNaNTest() {
        Instant[] firstTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] firstValuesArray = {24.6, 28.7, 26.3, 13.8, 27.4};
        ArrayList<Instant> firstTimestamps = new ArrayList<>(Arrays.asList(firstTimestampsArray));
        ArrayList<Double> firstValues = new ArrayList<>(Arrays.asList(firstValuesArray));
        TimeSeries<Double> first = new TimeSeries<>(firstTimestamps, firstValues, Double.class);

        Instant[] secondTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] secondValuesArray = {999.0, Double.NaN};
        ArrayList<Instant> secondTimestamps = new ArrayList<>(Arrays.asList(secondTimestampsArray));
        ArrayList<Double> secondValues = new ArrayList<>(Arrays.asList(secondValuesArray));
        TimeSeries<Double> second = new TimeSeries<>(secondTimestamps, secondValues, Double.class);

        Instant[] thirdTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] thirdValuesArray = {24.6, 28.7, 999.0, 13.8, 27.4};
        ArrayList<Instant> thirdTimestamps = new ArrayList<>(Arrays.asList(thirdTimestampsArray));
        ArrayList<Double> thirdValues = new ArrayList<>(Arrays.asList(thirdValuesArray));
        TimeSeries<Double> third = new TimeSeries<>(thirdTimestamps, thirdValues, Double.class);

        first.set(second, true);

        assertEquals(third, first);
    }

    @Test
    public void toIntegerTest() {
        Instant[] datetimesArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] doubleArray = {1., 2., 3., 4., 5., 6., 7.};
        Integer[] integerArray = {1, 2, 3, 4, 5, 6, 7};
        ArrayList<Instant> timestamps = new ArrayList<>(Arrays.asList(datetimesArray));
        ArrayList<Double> doubleValues = new ArrayList<>(Arrays.asList(doubleArray));
        ArrayList<Integer> integerValues = new ArrayList<>(Arrays.asList(integerArray));
        TimeSeries<Double> original = new TimeSeries<>(timestamps, doubleValues, Double.class);

        TimeSeries<Integer> expected = new TimeSeries<>(timestamps, integerValues, Integer.class);

        assertEquals(expected, original.toInteger());
    }

    @Test
    public void appendTest() {
        Instant[] firstTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC)};
        Double[] firstValuesArray = {24.6, 28.7, 26.3, 13.8, 27.4};
        ArrayList<Instant> firstTimestamps = new ArrayList<>(Arrays.asList(firstTimestampsArray));
        ArrayList<Double> firstValues = new ArrayList<>(Arrays.asList(firstValuesArray));
        TimeSeries<Double> first = new TimeSeries<>(firstTimestamps, firstValues, Double.class);

        Instant[] secondTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 19, 15).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 20, 55).toInstant(ZoneOffset.UTC)};
        Double[] secondValuesArray = {999.0, 666.6};
        ArrayList<Instant> secondTimestamps = new ArrayList<>(Arrays.asList(secondTimestampsArray));
        ArrayList<Double> secondValues = new ArrayList<>(Arrays.asList(secondValuesArray));
        TimeSeries<Double> second = new TimeSeries<>(secondTimestamps, secondValues, Double.class);

        Instant[] thirdTimestampsArray = {
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 55).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 12, 59).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 15, 28).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 17, 8).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 19, 15).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2015, Month.JANUARY, 5, 20, 55).toInstant(ZoneOffset.UTC)};
        Double[] thirdValuesArray = {24.6, 28.7, 26.3, 13.8, 27.4, 999.0, 666.6};
        ArrayList<Instant> thirdTimestamps = new ArrayList<>(Arrays.asList(thirdTimestampsArray));
        ArrayList<Double> thirdValues = new ArrayList<>(Arrays.asList(thirdValuesArray));
        TimeSeries<Double> third = new TimeSeries<>(thirdTimestamps, thirdValues, Double.class);

        first.append(second);

        assertEquals(third, first);
    }
}
