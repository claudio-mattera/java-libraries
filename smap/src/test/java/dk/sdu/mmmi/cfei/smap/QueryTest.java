package dk.sdu.mmmi.cfei.smap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cgim
 */
public class QueryTest {

    @Test
    public void beforeNowTest() {
        String actual = Query.beforeNow().toString();
        String expected = "select data before now";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowLimitTest() {
        String actual = Query.beforeNow().setLimit(200).toString();
        String expected = "select data before now limit 200";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowStreamLimitTest() {
        String actual = Query.beforeNow().setStreamLimit(1000).toString();
        String expected = "select data before now streamlimit 1000";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowBothLimitsTest() {
        String actual = Query.beforeNow().setStreamLimit(300).setLimit(2).toString();
        String expected = "select data before now limit 2 streamlimit 300";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowWhereIsTest() {
        String actual = Query.beforeNow().whereIs("uuid", "1234").toString();
        String expected = "select data before now where uuid = '1234'";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowWhereLikeTest() {
        String actual = Query.beforeNow().whereLike("path", "/first/%/third").toString();
        String expected = "select data before now where path like '/first/%/third'";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowMultipleWhereTest() {
        String actual = Query.beforeNow().whereLike("path", "/first/%/third").whereIs("uuid", "1234").toString();
        String expected = "select data before now where path like '/first/%/third' and uuid = '1234'";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeNowRawTest() {
        String actual = Query.beforeNow().whereRaw("path like '/first/%/third' or uuid = '1234'").toString();
        String expected = "select data before now where path like '/first/%/third' or uuid = '1234'";
        assertEquals(expected, actual);
    }

    @Test
    public void inWhereLikeTest() {
        LocalDateTime start = LocalDateTime.of(2014, Month.MARCH, 15, 23, 38);
        LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 6, 11, 6);
        String actual = Query.in(start, end).whereLike("path", "/first/%/third").toString();
        String expected = "select data in ('03/15/2014 23:38', '02/06/2015 11:06') where path like '/first/%/third'";
        assertEquals(expected, actual);
    }

    @Test
    public void inInstantWhereLikeTest() {
        Instant start = LocalDateTime.of(2014, Month.MARCH, 15, 23, 38).toInstant(ZoneOffset.UTC);
        Instant end = LocalDateTime.of(2015, Month.FEBRUARY, 6, 11, 6).toInstant(ZoneOffset.UTC);
        String actual = Query.in(start, end).whereLike("path", "/first/%/third").toString();
        String expected = "select data in (1394926680000, 1423220760000) where path like '/first/%/third'";
        assertEquals(expected, actual);
    }

    @Test
    public void afterWhereLikeTest() {
        LocalDateTime start = LocalDateTime.of(2014, Month.MARCH, 15, 23, 38);
        String actual = Query.after(start).whereLike("path", "/first/%/third").toString();
        String expected = "select data after '03/15/2014 23:38' where path like '/first/%/third'";
        assertEquals(expected, actual);
    }

    @Test
    public void beforeWhereLikeTest() {
        LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 6, 11, 6);
        String actual = Query.before(end).whereLike("path", "/first/%/third").toString();
        String expected = "select data before '02/06/2015 11:06' where path like '/first/%/third'";
        assertEquals(expected, actual);
    }
}
