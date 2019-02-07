package dk.sdu.mmmi.cfei.smap;

import dk.sdu.mmmi.cfei.dataframes.DataFrame;
import dk.sdu.mmmi.cfei.dataframes.TimeSeries;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * A class to post updates to a sMAP server.
 *
 * <pre>
 * {@code
 * SmapUpdater smapUpdater = new SmapUpdater(host, port, key, dryRun);
 *
 * Map<String, Object> metadata = new HashMap<>();
 * metadata.put("Location/City", "Amesbury");
 * metadata.put("SourceName", "Prehistory");
 *
 * TimeSeries<Double> readings = ...
 *
 * String sourceName = "Prehistory";
 * String path = "/path/to/stonehenge";
 *
 * JSONObject payload = SmapUpdater.generateUpdatePayload(
 *      sourceName,
 *      path,
 *      readings,
 *      Optional.of(metadata),
 *      Optional.empty);
 *
 * smapUpdater.postData(payload);
 * }
 * </pre>
 *
 * @author cgim
 * @see Payload
 */
public class SmapUpdater {

    /**
     * Create a sMAP updater.
     *
     * @param host sMAP host name.
     * @param port sMAP port.
     * @param key sMAP access key.
     * @param dryRun If true, only pretend to post updates.
     * @throws URISyntaxException If the host name is not valid.
     */
    public SmapUpdater(String host, int port, String key, boolean dryRun) throws URISyntaxException {
        smapUri = new URIBuilder()
                .setScheme("http")
                .setHost(host)
                .setPort(port)
                .setPath("/add/" + key)
                .build();
        this.dryRun = dryRun;
        LOGGER.log(
                Level.FINE,
                "sMAP URI: {0}{1}",
                new Object[]{smapUri, dryRun ? " (dry run)" : ""});

        this.smapFetcher = new SmapFetcher(host, port);

        this.pathsCache = new HashMap<>();
    }

    /**
     * Post new readings for the specified UUID.
     *
     * The stream's path is fetched automatically and cached.
     *
     * @param uuid Stream's UUID
     * @param readings Stream's readings
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     * @see #postReadings(java.util.HashMap)
     */
    public void postReadings(UUID uuid, TimeSeries<Double> readings) throws IOException {
        HashMap<UUID, TimeSeries<Double>> map = new HashMap<>();
        map.put(uuid, readings);
        this.postReadings(map);
    }

    /**
     * Post new readings for specified UUIDs.
     *
     * The streams' paths are fetched automatically and cached.
     *
     * @param map A mapping (UUID, readings)
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public void postReadings(HashMap<UUID, TimeSeries<Double>> map) throws IOException {
        final List<UUID> unknownUUIDs = new ArrayList<>();
        for (UUID uuid : map.keySet()) {
            if (!this.pathsCache.containsKey(uuid)) {
                unknownUUIDs.add(uuid);
            }
        }

        if (!unknownUUIDs.isEmpty()) {
            LOGGER.log(
                    Level.FINE,
                    "Fetching paths for {0} unknown UUIDs",
                    new Object[]{unknownUUIDs.size()}
            );

            final MetadataQuery query = MetadataQuery.valueOf("Path")
                    .whereRaw(
                            String.join(" or ", unknownUUIDs.stream()
                                    .map((UUID uuid) -> MessageFormat.format("uuid = ''{0}''", uuid))
                                    .collect(Collectors.toList()))
                    );

            Map<UUID, String> newPaths = this.smapFetcher.getMetadataStrings(query);
            this.pathsCache.putAll(newPaths);
        }

        JSONObject entirePayload = new JSONObject(map.entrySet().stream().map(entry
                -> SmapUpdater.generateUpdatePayload(
                        this.pathsCache.get(entry.getKey()),
                        entry.getValue(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(entry.getKey()),
                        Optional.empty()
                ))
                .collect(
                        Collectors.toMap(
                                (Payload payload) -> payload.path,
                                (Payload payload) -> payload.object)));

        this.postData(entirePayload);
    }

    /**
     * Post a JSON payload to the sMAP server.
     *
     * @param payload The JSON payload.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public void postData(JSONObject payload) throws IOException {
        this.postData(
                payload,
                Duration.ofSeconds(-1),
                Duration.ofSeconds(-1),
                Duration.ofSeconds(-1)
        );
    }

    /**
     * Post a JSON payload to the sMAP server specifying timeouts
     *
     * Timeouts will be converted to seconds and casted to int. A value of 0
     * means infinite timeout, a value of -1 seconds means system default
     * timeout.
     *
     * @param payload The JSON payload.
     * @param connectionRequestTimeout Maximum time to wait for a connection
     * from the connection manager
     * @param connectionTimeout Maximum time to establish the connection with
     * the remote host
     * @param socketTimeout Maximum time of inactivity between two data packets
     * after connection was established with the remote host
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     * @since 1.3.2
     */
    public void postData(
            JSONObject payload,
            Duration connectionRequestTimeout,
            Duration connectionTimeout,
            Duration socketTimeout
    ) throws IOException {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout((int) connectionRequestTimeout.toMillis())
                .setConnectTimeout((int) connectionTimeout.toMillis())
                .setSocketTimeout((int) socketTimeout.toMillis())
                .build();
        final HttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .build();
        postData(payload, httpClient);
    }

    /**
     * Post a stream of JSON payloads to the sMAP server.
     *
     * @param payloads The JSON payloads.
     * @throws IOException If an error occurs communicating with the sMAP
     * server.
     */
    public void postData(Stream<JSONObject> payloads) throws IOException {
        final HttpClient httpClient = HttpClientBuilder.create().build();
        Stream<Optional<Error>> allResults = payloads.map(payload -> {
            try {
                postData(payload, httpClient);
                return Optional.empty();
            } catch (IOException ex) {
                LOGGER.log(Level.SEVERE, "Got error: {0}", ex.getLocalizedMessage());
                return Optional.of(new Error(ex.getLocalizedMessage(), payload));
            }
        });
        List<Error> errors = allResults
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        if (!errors.isEmpty()) {
            String details = String.join(
                    "\n\n",
                    errors.stream()
                            .map(error -> {
                                return error.message + " - " + error.payload;
                            })
                            .collect(Collectors.toList()));
            throw new IOException("Errors in updating payloads: " + details);
        }
    }

    private void postData(JSONObject payload, HttpClient httpClient) throws IOException {
        final HttpPost request = new HttpPost(smapUri);
        final String content = payload.toString();
        LOGGER.log(Level.INFO, "Posting payload, length: {0} bytes", content.length());

        try {
            final StringEntity enparams = new StringEntity(content, "UTF-8");
            request.addHeader("content-type", "application/json");
            request.setEntity(enparams);
            if (!dryRun) {
                final HttpResponse response = httpClient.execute(request);
                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_OK) {
                    LOGGER.log(Level.SEVERE, "Response: {0}", response.toString());
                    throw new RuntimeException(
                            MessageFormat.format(
                                    "Updating sMAP failed with status code {0}",
                                    statusCode));
                }
            }
        } catch (UnsupportedEncodingException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Generate a JSON update.
     *
     * outputProperties should contain global and specialized metadata and
     * properties. Global values are applied to each stream, while specialized
     * values (in the form (columnName + "_" + valueName)) only to the
     * corresponding stream. At the bare minimum, there must be a value
     * (columnName + "_PATH") and a "Metadata/SourceName" (or the specialized
     * version (columnName + "_Metadata/SourceName")), which are used to
     * generate the stream UUID.
     *
     * An example properties file:
     *
     * <pre>
     * {@code
     * Metadata/SourceName=Building Data
     * Metadata/Type=Forecast
     * Metadata/Location/City=Odense
     * Metadata/Location/Latitude=55.4
     * Metadata/Location/Longitude=10.4
     * Properties/TimeZone=Europe/Copenhagen
     * Properties/UnitofTime=ms
     * temperature_after_heat_recovery_PATH=/Ventilation/Temperature after heat recovery
     * temperature_after_heat_recovery_Properties/UnitofMeasure=C
     * exhaust_air_temperature_PATH=/Ventilation/Exhaust air temperature
     * exhaust_air_temperature_Properties/UnitofMeasure=C
     * outdoor_temperature_PATH=/Ventilation/Outdoor temperature
     * outdoor_temperature_Properties/UnitofMeasure=C
     * inflow_air_rate_PATH=/Ventilation/Inflow air
     * inflow_air_rate_Properties/UnitofMeasure=m3/s
     * exhaust_air_rate_PATH=/Ventilation/Exhaust ait
     * exhaust_air_rate_Properties/UnitofMeasure=m3/s
     * }
     * </pre>
     *
     * @param outputProperties Output configuration.
     * @param dataframe Output data.
     * @return A JSON representing the update.
     */
    public static JSONObject generateUpdateJson(Properties outputProperties, DataFrame dataframe) {
        Map<String, Object> commonMetadata = outputProperties.keySet().stream()
                .map(obj -> (String) obj)
                .filter(name -> name.startsWith("Metadata/"))
                .map(name -> name.substring("Metadata/".length()))
                .collect(Collectors.toMap(
                        name -> name,
                        name -> outputProperties.getProperty("Metadata/" + name)));

        Map<String, Object> commonProperties = outputProperties.keySet().stream()
                .map(obj -> (String) obj)
                .filter(name -> name.startsWith("Properties/"))
                .map(name -> name.substring("Properties/".length()))
                .collect(Collectors.toMap(
                        name -> name,
                        name -> outputProperties.getProperty("Properties/" + name)));

        final String sourceName = (String) commonMetadata.get("SourceName");

        LOGGER.log(Level.INFO, "Computing update payload");
        JSONObject entirePayload = new JSONObject(dataframe.getColumns().stream()
                .filter(column -> outputProperties.containsKey(column.name + "_PATH"))
                .map(column -> {
                    LOGGER.log(Level.FINE, "Processing output column {0}", column);
                    String path = outputProperties.getProperty(column.name + "_PATH");
                    LOGGER.log(Level.FINE, "Path: {0}", path);

                    Map<String, Object> metadata = new HashMap<>(commonMetadata);
                    outputProperties.keySet().stream()
                            .map(obj -> (String) obj)
                            .filter(name -> name.startsWith(column.name + "_Metadata/"))
                            .map(name -> name.substring((column.name + "_Metadata/").length()))
                            .forEach(name -> {
                                metadata.put(name, outputProperties.getProperty(column.name + "_Metadata/" + name));
                            });

                    Map<String, Object> properties = new HashMap<>(commonProperties);
                    outputProperties.keySet().stream()
                            .map(obj -> (String) obj)
                            .filter(name -> name.startsWith(column.name + "_Properties/"))
                            .map(name -> name.substring((column.name + "_Properties/").length()))
                            .forEach(name -> {
                                properties.put(name, outputProperties.getProperty(column.name + "_Properties/" + name));
                            });

                    properties.put("ReadingType", column.type.getSimpleName().toLowerCase());

                    // Take also metadata and properties from dataframe.
                    column.metadata.keySet().stream()
                            .filter(key -> key.startsWith("Metadata/"))
                            .forEach(key -> {
                                metadata.put(
                                        key.substring("Metadata/".length()),
                                        column.metadata.get(key));
                            });
                    column.metadata.keySet().stream()
                            .filter(key -> key.startsWith("Properties/"))
                            .forEach(key -> {
                                properties.put(
                                        key.substring("Properties/".length()),
                                        column.metadata.get(key));
                            });

                    TimeSeries timeseries = dataframe.getColumn(column);

                    Optional<UUID> suppliedUuid
                            = Optional.ofNullable(
                                    outputProperties.getProperty(column.name + "_UUID"))
                                    .map(UUID::fromString);

                    return SmapUpdater.generateUpdatePayload(
                            sourceName, path, timeseries,
                            Optional.of(metadata), Optional.of(properties),
                            suppliedUuid);
                })
                .collect(Collectors.toMap(
                        (Payload payload) -> payload.path,
                        (Payload pair) -> pair.object)));
        return entirePayload;
    }

    /**
     * Generate a JSON update payload.
     *
     * @param sourceName The stream source name.
     * @param path The stream path.
     * @param readings The readings to post.
     * @param metadataOpt The metadata to post.
     * @param propertiesOpt The properties to post.
     * @param suppliedUuid Stream's UUID.
     * @return A JSON payload representing the update.
     * @deprecated Use {@link #generateUpdatePayload(String, TimeSeries, Optional, Optional, Optional, Optional)
     * } instead.
     */
    @Deprecated
    public static Payload generateUpdatePayload(
            String sourceName,
            String path,
            TimeSeries<Double> readings,
            Optional<Map<String, Object>> metadataOpt,
            Optional<Map<String, Object>> propertiesOpt,
            Optional<UUID> suppliedUuid) {
        UUID uuid = suppliedUuid.orElseGet(() -> generateUuid(sourceName, path));
        JSONObject inner = new JSONObject();
        inner.put("uuid", uuid);
        metadataOpt.ifPresent(metadata
                -> inner.put(
                        "Metadata",
                        convertMapsToJson(generateMapOfMaps(metadata))));
        propertiesOpt.ifPresent(properties
                -> inner.put(
                        "Properties",
                        convertMapsToJson(generateMapOfMaps(properties))));
        inner.put("Readings", convertTimeseriesToJson(readings));
        return new Payload(path, inner);
    }

    /**
     * Generate a JSON update payload.
     *
     * If UUID is not specified, a deterministic UUID will be generated from
     * source name and path.
     *
     * @param path The stream path.
     * @param readings The readings to post.
     * @param metadataOpt The metadata to post.
     * @param propertiesOpt The properties to post.
     * @param suppliedUuid Stream's UUID.
     * @param sourceName The stream source name.
     * @return A JSON payload representing the update.
     */
    public static Payload generateUpdatePayload(
            String path,
            TimeSeries<Double> readings,
            Optional<Map<String, Object>> metadataOpt,
            Optional<Map<String, Object>> propertiesOpt,
            Optional<UUID> suppliedUuid,
            Optional<String> sourceName) {
        UUID uuid = suppliedUuid.orElseGet(() -> generateUuid(sourceName.get(), path));
        JSONObject inner = new JSONObject();
        inner.put("uuid", uuid);
        metadataOpt.ifPresent(metadata
                -> inner.put(
                        "Metadata",
                        convertMapsToJson(generateMapOfMaps(metadata))));
        propertiesOpt.ifPresent(properties
                -> inner.put(
                        "Properties",
                        convertMapsToJson(generateMapOfMaps(properties))));
        inner.put("Readings", convertTimeseriesToJson(readings));
        return new Payload(path, inner);
    }

    /**
     * Convert a time series to a JSON list of lists.
     *
     * The result is a list of 2-elements list, each containing a timestamp in
     * ms and a value.
     *
     * @param timeseries The time series.
     * @return A JSON list of lists.
     */
    public static JSONArray convertTimeseriesToJson(TimeSeries<Double> timeseries) {
        List<JSONArray> readingsList = timeseries.stream()
                .map(reading -> {
                    return new JSONArray(
                            Arrays.asList(
                                    reading.getDatetime().toEpochMilli(),
                                    reading.getValue()));
                })
                .collect(Collectors.toList());

        return new JSONArray(readingsList);
    }

    /**
     * Convert a map of maps to JSON.
     *
     * @param original A map of maps.
     * @return A JSON object.
     */
    public static JSONObject convertMapsToJson(Map<String, Object> original) {
        JSONObject result = new JSONObject();
        original.entrySet().forEach(entry -> {
            if (entry instanceof Map) {
                Map<String, Object> child = (Map<String, Object>) entry.getValue();
                result.put(entry.getKey(), convertMapsToJson(child));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        });
        return result;
    }

    /**
     * Generates a map of maps from a map of strings.
     *
     * Example: input:
     * <pre>
     * {@code
     * "One": 1
     * "Two/Three": 23
     * "Four/Five/Six": 456
     * "Two/Seven": 27
     * }
     * </pre>
     *
     * Output:
     * <pre>
     * {@code
     * "One": 1
     * "Two":
     *     "Three": 23
     *     "Seven": 27
     * "Four":
     *     "Five":
     *         "Six": 456
     * }
     * </pre>
     *
     * @param original The map of strings.
     * @return A map of maps.
     */
    public static Map<String, Object> generateMapOfMaps(Map<String, Object> original) {
        return generateMapOfMaps(original, 0, Arrays.asList());
    }

    private static Map<String, Object> generateMapOfMaps(Map<String, Object> original, int level, List<String> currentBranch) {
        Map<String, Object> results = new HashMap<>();

        original.keySet().stream()
                .map(key -> Arrays.asList(key.split("/")))
                .filter(tokens -> tokens.size() > level)
                .filter(tokens -> tokens.subList(0, level).equals(currentBranch))
                .forEach(tokens -> {
                    String completeKey = String.join("/", tokens.subList(0, level + 1));
                    String newKey = tokens.get(level);
                    if (tokens.size() == level + 1) {
                        Object value = original.get(completeKey);
                        results.put(newKey, value);
                    } else {
                        List<String> newBranch = new ArrayList<>(currentBranch);
                        newBranch.add(newKey);
                        Object value = generateMapOfMaps(original, level + 1, newBranch);
                        results.put(newKey, value);
                    }
                });

        return results;
    }

    public static UUID generateUuid(String sourceName, String path) {
        try {
            assert sourceName != null;
            assert path != null;

            final String base = sourceName + path;
            final byte[] hash = MessageDigest
                    .getInstance("MD5").digest(base.getBytes("UTF-8"));
            return UUID.nameUUIDFromBytes(hash);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            LOGGER.log(
                    Level.SEVERE,
                    "Cannot generate UUID: {0}",
                    ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    class Error {

        public Error(String message, JSONObject payload) {
            this.message = message;
            this.payload = payload;
        }

        public final String message;
        public final JSONObject payload;
    }

    private final URI smapUri;
    private final boolean dryRun;
    private final SmapFetcher smapFetcher;
    private final Map<UUID, String> pathsCache;

    private static final Logger LOGGER
            = Logger.getLogger(SmapUpdater.class.getName());
}
