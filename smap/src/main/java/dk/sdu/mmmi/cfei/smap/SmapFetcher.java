package dk.sdu.mmmi.cfei.smap;

import dk.sdu.mmmi.cfei.dataframes.Reading;
import dk.sdu.mmmi.cfei.dataframes.TimeSeries;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * A class to fetch data from a sMAP server.
 *
 * <pre>
 * {@code
 * Query query = Query.after(LocalDateTime.of(...))
 *      .setLimit(200)
 *      .whereLike("path", "/path/to/room%")
 *      .whereIs("Metadata/SourceName", "Some source name");
 * SmapFetcher smapFetcher = new SmapFetcher(hostname, port, key);
 * smapFetcher.execute(query);
 * }
 * </pre>
 *
 * @author cgim
 * @see Query
 */
public class SmapFetcher {

    /**
     * Create a sMAP fetcher.
     *
     * @param host sMAP host name.
     * @param port sMAP port.
     * @throws URISyntaxException If the host name is not valid.
     */
    public SmapFetcher(String host, int port) throws URISyntaxException {
        this.uri = new URIBuilder()
                .setScheme("http")
                .setHost(host)
                .setPort(port)
                .setPath("/api/query")
                .build();
    }

    /**
     * Create a sMAP fetcher.
     *
     * @param host sMAP host name.
     * @param port sMAP port.
     * @param key sMAP access key.
     * @throws URISyntaxException If the host name is not valid.
     * @deprecated Use {@link #SmapFetcher(String, int)} instead.
     */
    @Deprecated
    public SmapFetcher(String host, int port, String key) throws URISyntaxException {
        this.uri = new URIBuilder()
                .setScheme("http")
                .setHost(host)
                .setPort(port)
                .setPath("/api/query")
                .setParameter("key", key)
                .build();
    }

    /**
     * Execute a query to retrieve raw metadata.
     *
     * @param query The query.
     * @return A JSON array containing the metadata.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public JSONArray getRawMetadata(MetadataQuery query) throws IOException {
        return parseJSONArray(postQuery(query.toString()));
    }

    /**
     * Execute a query to retrieve a single metadata string.
     *
     * This method expects sMAP to reply with a single stream and that the
     * requested field is a string.
     *
     * @param query The query.
     * @return A string.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public String getMetadataSingleString(MetadataQuery query) throws IOException {
        JSONArray array = parseJSONArray(postQuery(query.toString()));
        assert array.length() == 1;
        final List<String> fields = Arrays.asList(query.getPath().split("/"));
        JSONObject o = array.getJSONObject(0);
        for (String field : fields.subList(0, fields.size() - 1)) {
            o = o.getJSONObject(field);
        }
        return o.getString(fields.get(fields.size() - 1));
    }

    /**
     * Execute a query to retrieve metadata strings.
     *
     * This method expects that the requested field is a string (not a
     * dictionary).
     *
     * @param query The query.
     * @return A mapping (UUID, field), one per stream.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public Map<UUID, String> getMetadataStrings(MetadataQuery query) throws IOException {
        return this.getMetadataCustom(query, (object, name) -> object.getString(name));
    }

    /**
     * Execute a query to retrieve metadata integers.
     *
     * This method expects that the requested field is an integer (not a
     * dictionary).
     *
     * @param query The query.
     * @return A mapping (UUID, field), one per stream.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     * @throws org.json.JSONException If the requested field is not an integer.
     */
    public Map<UUID, Integer> getMetadataIntegers(MetadataQuery query) throws IOException {
        return this.getMetadataCustom(query, (object, name) -> object.getInt(name));
    }

    /**
     * Execute a query to retrieve metadata doubles.
     *
     * This method expects that the requested field is a double (not a
     * dictionary).
     *
     * @param query The query.
     * @return A mapping (UUID, field), one per stream.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     * @throws org.json.JSONException If the requested field is not a double.
     */
    public Map<UUID, Double> getMetadataDoubles(MetadataQuery query) throws IOException {
        return this.getMetadataCustom(query, (object, name) -> object.getDouble(name));
    }

    private <T> Map<UUID, T> getMetadataCustom(
            MetadataQuery query,
            BiFunction<JSONObject, String, T> f
    ) throws IOException {
        List<String> pathElements = Arrays.asList(query.getPath().split("/"));
        final int n = pathElements.size();
        final JSONArray array = parseJSONArray(postQuery(query.toString()));
        return IntStream.range(0, array.length())
                .mapToObj(i -> array.getJSONObject(i))
                .collect(Collectors.toMap(
                        object -> UUID.fromString(object.getString("uuid")),
                        object -> {
                            for (String element : pathElements.subList(0, n - 1)) {
                                object = object.getJSONObject(element);
                            }
                            return f.apply(object, pathElements.get(n - 1));
                        }
                ));
    }

    /**
     * Execute a query to retrieve readings.
     *
     * @param query The query.
     * @return A mapping (UUID, Time series).
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public Map<UUID, TimeSeries> execute(Query query) throws IOException {
        return execute(query.toString());
    }

    /**
     * Get the raw JSON metadata trees for each UUID.
     *
     * Note: the metadata trees contain the following objects: Metadata,
     * Properties, uuid and Path.
     *
     * @param query The query. Its path MUST be "*".
     * @return A mapping (UUID, metadata tree)
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public Map<UUID, JSONObject> getMetadataTrees(MetadataQuery query)
            throws IOException {

        assert query.getPath().equals("*");

        final JSONArray array = parseJSONArray(postQuery(query.toString()));

        return IntStream.range(0, array.length())
                .mapToObj(i -> array.getJSONObject(i))
                .collect(Collectors.toMap(
                        object -> UUID.fromString(object.getString("uuid")),
                        object -> object
                ));
    }

    /**
     * Execute a query to retrieve readings.
     *
     * @param query The query.
     * @return A mapping (UUID, Time series), where the time series data type is
     * guessed from the JSON returned by sMAP server.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public Map<UUID, TimeSeries> execute(String query) throws IOException {
        return jsonArrayToTimeSeriesMap(parseJSONArray(postQuery(query)), Optional.empty());
    }

    /**
     * Execute a query to retrieve readings.
     *
     * @param query The query.
     * @param dtype Specify the returned time series data type.
     * @return A mapping (UUID, Time series).
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public Map<UUID, TimeSeries> execute(String query, Class dtype) throws IOException {
        return jsonArrayToTimeSeriesMap(parseJSONArray(postQuery(query)), Optional.of(dtype));
    }

    private String postQuery(String query) throws IOException {
        final HttpClient httpClient = HttpClientBuilder.create()
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build();
        return postQuery(query, httpClient);
    }

    private String postQuery(String query, HttpClient httpClient) throws IOException {
        LOGGER.log(Level.FINE, "Posting query: {0}", query);

        final HttpPost request = new HttpPost(uri);
        final StringEntity enparams
                = new StringEntity(query, "UTF-8");
        request.addHeader("content-type", "text/plain");
        request.addHeader("charset", "utf-8");
        request.setEntity(enparams);
        final HttpResponse response = httpClient.execute(request);
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            return readStream(response.getEntity().getContent());
        } else {
            final String output = readStream(response.getEntity().getContent());
            throw new RuntimeException(
                    MessageFormat.format(
                            "HTTP Status Code {0}: {1}", statusCode, output
                    )
            );
        }
    }

    private static Map<UUID, TimeSeries> jsonArrayToTimeSeriesMap(JSONArray array, Optional<Class> dtype) {
        Map<UUID, TimeSeries> results = new HashMap<>();

        for (Object object : array) {
            JSONObject jsonObject = (JSONObject) object;
            UUID uuid = UUID.fromString((String) jsonObject.get("uuid"));
            JSONArray readingsArray = (JSONArray) jsonObject.get("Readings");

            // Note: this is unreliable, sometimes sMAP server returns integer
            // numbers "encoded" as floats (i.e., with a trailing .0)
            Class clazz = dtype.orElseGet(() -> guessDataType(readingsArray));

            TimeSeries timeseries = new TimeSeries<>(clazz);
            for (Object readingArrayObject : readingsArray) {
                JSONArray readingArray = (JSONArray) readingArrayObject;
                final long timestamp = ((Number) readingArray.get(0)).longValue();
                final Object value = readingArray.get(1);
                timeseries.addReading(new Reading<>(Instant.ofEpochMilli(timestamp), value, clazz));
            }
            results.put(uuid, timeseries);
        }
        return results;
    }

    private static JSONArray parseJSONArray(String string) {
        return new JSONArray(string);
    }

    private static String readStream(InputStream inputStream) {
        Scanner scanner = new Scanner(inputStream).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }

    private static Class guessDataType(JSONArray readingsArray) {
        Object readingArrayObject = readingsArray.get(0);
        JSONArray readingArray = (JSONArray) readingArrayObject;
        Object valueObject = readingArray.get(1);
        Class dtype = valueObject.getClass();
        LOGGER.log(Level.FINEST, "Guessing data type: {0}", dtype);
        return dtype;
    }

    private final URI uri;

    private static final Logger LOGGER
            = Logger.getLogger(SmapFetcher.class.getName());
}
