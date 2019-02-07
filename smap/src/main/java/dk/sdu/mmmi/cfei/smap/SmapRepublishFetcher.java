package dk.sdu.mmmi.cfei.smap;

import dk.sdu.mmmi.cfei.dataframes.Reading;
import dk.sdu.mmmi.cfei.dataframes.TimeSeries;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * A class to fetch data from a sMAP server through the republish API.
 *
 * When the client makes a HTTP call to the republish API, sMAP returns an
 * infinite stream of results, little by little, as soon as they are available.
 *
 * @author cgim
 */
public class SmapRepublishFetcher {

    /**
     * Create a sMAP republish fetcher.
     *
     * @param host sMAP host name.
     * @param port sMAP port.
     * @throws URISyntaxException If the host name is not valid.
     */
    public SmapRepublishFetcher(String host, int port) throws URISyntaxException {
        this.uri = new URIBuilder()
                .setScheme("http")
                .setHost(host)
                .setPort(port)
                .setPath("/republish")
                .build();
    }

    /**
     * Execute a query to retrieve readings.
     *
     * The returned stream contains multiple mapping for a single UUID. The
     * calling code should concatenate the time series for the same UUID.
     *
     * @param query The query.
     * @return A stream of mappings (UUID, update).
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public Stream<Map.Entry<UUID, TimeSeries>> execute(String query) throws IOException {
        final HttpClient httpClient = HttpClientBuilder.create().build();
        LOGGER.finer("Getting lines...");
        Stream<String> lines = postQuery(query, httpClient);
        LOGGER.finer("Mapping JSONObject ctor to lines...");
        Stream<JSONObject> jsonObjects = lines
                .filter(line -> !line.isEmpty())
                .map(line -> {
                    LOGGER.log(Level.FINEST, "Received line: {0}", line);
                    return new JSONObject(line);
                })
                .filter(outerJsonObject -> {
                    String path = outerJsonObject.keySet().iterator().next();
                    JSONObject jsonObject = outerJsonObject.getJSONObject(path);
                    return jsonObject.has("uuid") && jsonObject.has("Readings");
                });
        LOGGER.finer("Mapping jsonObjectToPair to JSONObjects...");
        return jsonObjects.map(SmapRepublishFetcher::jsonObjectToEntry);
    }

    private Stream<String> postQuery(String query, HttpClient httpClient) throws IOException {
        LOGGER.log(Level.FINER, "Posting query to uri {0}", this.uri);
        final HttpPost request = new HttpPost(uri);
        final StringEntity enparams
                = new StringEntity(query, "UTF-8");
        request.addHeader("content-type", "text/plain");
        request.addHeader("charset", "utf-8");
        request.setEntity(enparams);
        final HttpResponse response = httpClient.execute(request);
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
            return reader.lines();
        } else {
            throw new RuntimeException(String.valueOf(statusCode));
        }
    }

    private static Map.Entry<UUID, TimeSeries> jsonObjectToEntry(JSONObject outerJsonObject) {
        String path = outerJsonObject.keySet().iterator().next();

        JSONObject jsonObject = outerJsonObject.getJSONObject(path);
        UUID uuid = UUID.fromString(jsonObject.getString("uuid"));

        JSONArray readingsArray = (JSONArray) jsonObject.get("Readings");

        Class clazz = guessDataType(readingsArray);

        TimeSeries timeseries = new TimeSeries<>(clazz);
        for (Object readingArrayObject : readingsArray) {
            JSONArray readingArray = (JSONArray) readingArrayObject;
            final long timestamp = ((Number) readingArray.get(0)).longValue();
            final Object value = readingArray.get(1);
            timeseries.addReading(new Reading<>(Instant.ofEpochMilli(timestamp), value, clazz));
        }

        return new AbstractMap.SimpleImmutableEntry<>(uuid, timeseries);
    }

    private static Class guessDataType(JSONArray readingsArray) {
        Object readingArrayObject = readingsArray.get(0);
        JSONArray readingArray = (JSONArray) readingArrayObject;
        Object valueObject = readingArray.get(1);
        return valueObject.getClass();
    }

    private final URI uri;

    private static final Logger LOGGER
            = Logger.getLogger(SmapRepublishFetcher.class.getName());
}
