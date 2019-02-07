package dk.sdu.mmmi.cfei.dataframes;

import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cgim
 */
public class DataFrameTest {

    @Test
    public void downsampleTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Double[] secondOriginalArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Double> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame original = new DataFrame(timestampsOriginal);
        original.addColumn(new Measure("first", Double.class), firstOriginal);
        original.addColumn(new Measure("second", Double.class), secondOriginal);

        Instant[] timestampsExpectedArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstExpectedArray = {1., 3., 5., 7.};
        Double[] secondExpectedArray = {9., 7., 5., 3.};
        List<Instant> timestampsExpected = Arrays.asList(timestampsExpectedArray);
        List<Double> firstExpected = Arrays.asList(firstExpectedArray);
        List<Double> secondExpected = Arrays.asList(secondExpectedArray);

        DataFrame expected = new DataFrame(timestampsExpected);
        expected.addColumn(new Measure("first", Double.class), firstExpected);
        expected.addColumn(new Measure("second", Double.class), secondExpected);

        DataFrame resampled = original.resample(Duration.ofMinutes(20));

        assertEquals(expected, resampled);
    }

    @Test
    public void appendTest() {
        Instant[] originalTimestampArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] originalFirstArray = {1., 2., 3., 4., 5., 6., 7.};
        Double[] originalSecondArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> originalTimestamps = Arrays.asList(originalTimestampArray);
        List<Double> originalFirst = Arrays.asList(originalFirstArray);
        List<Double> originalSecond = Arrays.asList(originalSecondArray);

        DataFrame original = new DataFrame(originalTimestamps);
        original.addColumn(new Measure("first", Double.class), originalFirst);
        original.addColumn(new Measure("second", Double.class), originalSecond);

        Instant[] additionTimestampArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 14, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 14, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 15, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 16, 0).toInstant(ZoneOffset.UTC),};
        Double[] additionFirstArray = {1., 3., 5., 7.};
        Double[] additionSecondArray = {9., 7., 5., 3.};
        List<Instant> additionTimestamps = Arrays.asList(additionTimestampArray);
        List<Double> additionFirst = Arrays.asList(additionFirstArray);
        List<Double> additionSecond = Arrays.asList(additionSecondArray);

        DataFrame addition = new DataFrame(additionTimestamps);
        addition.addColumn(new Measure("first", Double.class), additionFirst);
        addition.addColumn(new Measure("second", Double.class), additionSecond);

        Instant[] completeTimestampArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 14, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 14, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 15, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 16, 0).toInstant(ZoneOffset.UTC),};
        Double[] completeFirstArray = {1., 2., 3., 4., 5., 6., 7., 1., 3., 5., 7.};
        Double[] completeSecondArray = {9., 8., 7., 6., 5., 4., 3., 9., 7., 5., 3.};
        List<Instant> completeTimestamps = Arrays.asList(completeTimestampArray);
        List<Double> completeFirst = Arrays.asList(completeFirstArray);
        List<Double> completeSecond = Arrays.asList(completeSecondArray);

        DataFrame complete = new DataFrame(completeTimestamps);
        complete.addColumn(new Measure("first", Double.class), completeFirst);
        complete.addColumn(new Measure("second", Double.class), completeSecond);

        original.append(addition);

        assertEquals(complete, original);
    }

    @Test(expected = RuntimeException.class)
    public void appendDifferentColumnsTest() {
        Instant[] originalTimestampArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] originalFirstArray = {1., 2., 3., 4., 5., 6., 7.};
        Double[] originalSecondArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> originalTimestamps = Arrays.asList(originalTimestampArray);
        List<Double> originalFirst = Arrays.asList(originalFirstArray);
        List<Double> originalSecond = Arrays.asList(originalSecondArray);

        DataFrame original = new DataFrame(originalTimestamps);
        original.addColumn(new Measure("first", Double.class), originalFirst);
        original.addColumn(new Measure("second", Double.class), originalSecond);

        Instant[] additionTimestampArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 14, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 14, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 15, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 16, 0).toInstant(ZoneOffset.UTC),};
        Double[] additionFirstArray = {1., 3., 5., 7.};
        Double[] additionSecondArray = {9., 7., 5., 3.};
        List<Instant> additionTimestamps = Arrays.asList(additionTimestampArray);
        List<Double> additionFirst = Arrays.asList(additionFirstArray);
        List<Double> additionSecond = Arrays.asList(additionSecondArray);

        DataFrame addition = new DataFrame(additionTimestamps);
        addition.addColumn(new Measure("first", Double.class), additionFirst);
        addition.addColumn(new Measure("third", Double.class), additionSecond);

        original.append(addition);
    }

    @Test
    public void upsampleTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 3., 5., 7.};
        Double[] secondOriginalArray = {9., 7., 5., 3.};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Double> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame original = new DataFrame(timestampsOriginal);
        original.addColumn(new Measure("first", Double.class), firstOriginal);
        original.addColumn(new Measure("second", Double.class), secondOriginal);

        Instant[] timestampsExpectedArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstExpectedArray = {1., 2., 3., 4., 5., 6., 7.};
        Double[] secondExpectedArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> timestampsExpected = Arrays.asList(timestampsExpectedArray);
        List<Double> firstExpected = Arrays.asList(firstExpectedArray);
        List<Double> secondExpected = Arrays.asList(secondExpectedArray);

        DataFrame expected = new DataFrame(timestampsExpected);
        expected.addColumn(new Measure("first", Double.class), firstExpected);
        expected.addColumn(new Measure("second", Double.class), secondExpected);

        DataFrame resampled = original.resample(Duration.ofMinutes(10));

        assertEquals(expected, resampled);
    }

    @Test
    public void resampleSubsetTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Double[] secondOriginalArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Double> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame original = new DataFrame(timestampsOriginal);
        original.addColumn(new Measure("first", Double.class), firstOriginal);
        original.addColumn(new Measure("second", Double.class), secondOriginal);

        Instant[] timestampsExpectedArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),};
        Double[] firstExpectedArray = {2., 4., 6.};
        Double[] secondExpectedArray = {8., 6., 4.};
        List<Instant> timestampsExpected = Arrays.asList(timestampsExpectedArray);
        List<Double> firstExpected = Arrays.asList(firstExpectedArray);
        List<Double> secondExpected = Arrays.asList(secondExpectedArray);

        DataFrame expected = new DataFrame(timestampsExpected);
        expected.addColumn(new Measure("first", Double.class), firstExpected);
        expected.addColumn(new Measure("second", Double.class), secondExpected);

        DataFrame resampled = original.resample(
                LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
                LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
                Duration.ofMinutes(20));

        assertEquals(expected, resampled);
    }

    @Test
    public void heterogeneousDataTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Integer[] secondOriginalArray = {9, 8, 7, 6, 5, 4, 3};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Integer> secondOriginal = Arrays.asList(secondOriginalArray);
        TimeSeries<Double> firstTimeseries = new TimeSeries<>(new ArrayList<>(timestampsOriginal), new ArrayList<>(firstOriginal), Double.class);
        TimeSeries<Integer> secondTimeseries = new TimeSeries<>(new ArrayList<>(timestampsOriginal), new ArrayList<>(secondOriginal), Integer.class);

        DataFrame dataframe = new DataFrame(timestampsOriginal);
        dataframe.addColumn(new Measure("first", Double.class), firstOriginal);
        dataframe.addColumn(new Measure("second", Integer.class), secondOriginal);

        TimeSeries firstActual = dataframe.getColumn(new Measure("first", Double.class));
        TimeSeries secondActual = dataframe.getColumn(new Measure("second", Integer.class));

        assertEquals(firstTimeseries, firstActual);
        assertEquals(secondTimeseries, secondActual);
    }

    @Test
    public void heterogeneousMultipleReadingTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Integer[] secondOriginalArray = {9, 8, 7, 6, 5, 4, 3};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Integer> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame dataframe = new DataFrame(timestampsOriginal);
        dataframe.addColumn(new Measure("first", Double.class), firstOriginal);
        dataframe.addColumn(new Measure("second", Integer.class), secondOriginal);

        MultipleReading expected = new MultipleReading(LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC), Arrays.asList(3., 7));
        MultipleReading actual = dataframe.getRow(2);

        assertEquals(expected, actual);
    }

    @Test
    public void toCsvTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Integer[] secondOriginalArray = {9, 8, 7, 6, 5, 4, 3};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Integer> secondOriginal = Arrays.asList(secondOriginalArray);
        TimeSeries<Double> firstTimeseries = new TimeSeries<>(new ArrayList<>(timestampsOriginal), new ArrayList<>(firstOriginal), Double.class);
        TimeSeries<Integer> secondTimeseries = new TimeSeries<>(new ArrayList<>(timestampsOriginal), new ArrayList<>(secondOriginal), Integer.class);

        DataFrame dataframe = new DataFrame(timestampsOriginal);
        dataframe.addColumn(new Measure("first", Double.class), firstOriginal);
        dataframe.addColumn(new Measure("second", Integer.class), secondOriginal);

        String expected = "datetime,first,second\n"
                + "2016-03-15T12:00:00Z,1.0,9\n"
                + "2016-03-15T12:10:00Z,2.0,8\n"
                + "2016-03-15T12:20:00Z,3.0,7\n"
                + "2016-03-15T12:30:00Z,4.0,6\n"
                + "2016-03-15T12:40:00Z,5.0,5\n"
                + "2016-03-15T12:50:00Z,6.0,4\n"
                + "2016-03-15T13:00:00Z,7.0,3";

        String actual = dataframe.toCsv();

        assertEquals(expected, actual);
    }

    @Test
    public void toCsvNanTest() {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., Double.NaN, 6., 7.};
        Integer[] secondOriginalArray = {9, 8, 7, 6, 5, 4, 3};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Integer> secondOriginal = Arrays.asList(secondOriginalArray);
        TimeSeries<Double> firstTimeseries = new TimeSeries<>(new ArrayList<>(timestampsOriginal), new ArrayList<>(firstOriginal), Double.class);
        TimeSeries<Integer> secondTimeseries = new TimeSeries<>(new ArrayList<>(timestampsOriginal), new ArrayList<>(secondOriginal), Integer.class);

        DataFrame dataframe = new DataFrame(timestampsOriginal);
        dataframe.addColumn(new Measure("first", Double.class), firstOriginal);
        dataframe.addColumn(new Measure("second", Integer.class), secondOriginal);

        String expected = "datetime,first,second\n"
                + "2016-03-15T12:00:00Z,1.0,9\n"
                + "2016-03-15T12:10:00Z,2.0,8\n"
                + "2016-03-15T12:20:00Z,3.0,7\n"
                + "2016-03-15T12:30:00Z,4.0,6\n"
                + "2016-03-15T12:40:00Z,,5\n"
                + "2016-03-15T12:50:00Z,6.0,4\n"
                + "2016-03-15T13:00:00Z,7.0,3";

        String actual = dataframe.toCsv();

        assertEquals(expected, actual);
    }

    @Test
    public void fromCsvDoubleTest() throws IOException {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Double[] secondOriginalArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Double> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame expected = new DataFrame(timestampsOriginal);
        expected.addColumn(new Measure("first", Double.class), firstOriginal);
        expected.addColumn(new Measure("second", Double.class), secondOriginal);

        String csv = "datetime,first,second\n"
                + "2016-03-15 12:00:00,1.0,9.0\n"
                + "2016-03-15 12:10:00,2.0,8.0\n"
                + "2016-03-15 12:20:00,3.0,7.0\n"
                + "2016-03-15 12:30:00,4.0,6.0\n"
                + "2016-03-15 12:40:00,5.0,5.0\n"
                + "2016-03-15 12:50:00,6.0,4.0\n"
                + "2016-03-15 13:00:00,7.0,3.0";

        DataFrame actual = DataFrame.fromCsv(new StringReader(csv),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                ZoneOffset.UTC);

        assertEquals(expected, actual);
    }

    @Test
    public void fromCsvDoubleNanTest() throws IOException {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., Double.NaN, 6., 7.};
        Double[] secondOriginalArray = {9., 8., 7., 6., 5., 4., 3.};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Double> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame expected = new DataFrame(timestampsOriginal);
        expected.addColumn(new Measure("first", Double.class), firstOriginal);
        expected.addColumn(new Measure("second", Double.class), secondOriginal);

        String csv = "datetime,first,second\n"
                + "2016-03-15 12:00:00,1.0,9.0\n"
                + "2016-03-15 12:10:00,2.0,8.0\n"
                + "2016-03-15 12:20:00,3.0,7.0\n"
                + "2016-03-15 12:30:00,4.0,6.0\n"
                + "2016-03-15 12:40:00,,5.0\n"
                + "2016-03-15 12:50:00,6.0,4.0\n"
                + "2016-03-15 13:00:00,7.0,3.0";

        DataFrame actual = DataFrame.fromCsv(new StringReader(csv),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                ZoneOffset.UTC);

        assertEquals(expected, actual);
    }

    @Test
    public void fromCsvIntegerTest() throws IOException {
        Instant[] timestampsOriginalArray = {
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 10).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 20).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 30).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 40).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 12, 50).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2016, Month.MARCH, 15, 13, 0).toInstant(ZoneOffset.UTC),};
        Double[] firstOriginalArray = {1., 2., 3., 4., 5., 6., 7.};
        Integer[] secondOriginalArray = {9, 8, 7, 6, 5, 4, 3};
        List<Instant> timestampsOriginal = Arrays.asList(timestampsOriginalArray);
        List<Double> firstOriginal = Arrays.asList(firstOriginalArray);
        List<Integer> secondOriginal = Arrays.asList(secondOriginalArray);

        DataFrame expected = new DataFrame(timestampsOriginal);
        expected.addColumn(new Measure("first", Double.class), firstOriginal);
        expected.addColumn(new Measure("second", Integer.class), secondOriginal);

        String csv = "datetime,first,second\n"
                + "2016-03-15 12:00:00,1.0,9\n"
                + "2016-03-15 12:10:00,2.0,8\n"
                + "2016-03-15 12:20:00,3.0,7\n"
                + "2016-03-15 12:30:00,4.0,6\n"
                + "2016-03-15 12:40:00,5.0,5\n"
                + "2016-03-15 12:50:00,6.0,4\n"
                + "2016-03-15 13:00:00,7.0,3";

        DataFrame actual = DataFrame.fromCsv(new StringReader(csv),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                ZoneOffset.UTC,
                Arrays.asList(Double.class, Integer.class));

        assertEquals(expected, actual);
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

        DataFrame original = new DataFrame(firstTimestamps);
        Measure measure = new Measure("first", Double.class);
        original.addColumn(measure, firstValues);

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

        DataFrame expected = new DataFrame(thirdTimestamps);
        expected.addColumn(new Measure("first", Double.class), thirdValues);

        original.setContiguous(measure, second);

        assertEquals(expected, original);
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

        DataFrame original = new DataFrame(firstTimestamps);
        Measure measure = new Measure("first", Double.class);
        original.addColumn(measure, firstValues);

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

        DataFrame expected = new DataFrame(thirdTimestamps);
        expected.addColumn(new Measure("first", Double.class), thirdValues);

        original.set(measure, second, false);

        assertEquals(expected, original);
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

        DataFrame original = new DataFrame(firstTimestamps);
        Measure measure = new Measure("first", Double.class);
        original.addColumn(measure, firstValues);

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

        DataFrame expected = new DataFrame(thirdTimestamps);
        expected.addColumn(new Measure("first", Double.class), thirdValues);

        original.set(measure, second, true);

        assertEquals(expected, original);
    }
}
