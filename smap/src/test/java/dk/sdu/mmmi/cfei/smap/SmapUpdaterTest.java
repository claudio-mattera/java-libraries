package dk.sdu.mmmi.cfei.smap;

import dk.sdu.mmmi.cfei.dataframes.DataFrame;
import dk.sdu.mmmi.cfei.dataframes.Measure;
import dk.sdu.mmmi.cfei.dataframes.Reading;
import dk.sdu.mmmi.cfei.dataframes.TimeSeries;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author cgim
 */
public class SmapUpdaterTest {

    @Test
    public void generateMapOfMapsTest() {
        Map<String, Object> original = new HashMap<>();
        original.put("One", 1);
        original.put("Two/Three", 23);
        original.put("Four/Five/Six", 456);
        original.put("Two/Seven", 27);

        Map<String, Object> actual = SmapUpdater.generateMapOfMaps(original);

        Map<String, Object> expectedTwo = new HashMap<>();
        expectedTwo.put("Three", 23);
        expectedTwo.put("Seven", 27);

        Map<String, Object> expectedFive = new HashMap<>();
        expectedFive.put("Six", 456);

        Map<String, Object> expectedFour = new HashMap<>();
        expectedFour.put("Five", expectedFive);

        Map<String, Object> expectedOne = new HashMap<>();
        expectedOne.put("One", 1);
        expectedOne.put("Two", expectedTwo);
        expectedOne.put("Four", expectedFour);

        assertEquals(expectedOne, actual);
    }

    @Test
    public void convertMapsToJsonTest() {
        Map<String, Object> originalTwo = new HashMap<>();
        originalTwo.put("Three", 23);
        originalTwo.put("Seven", 27);

        Map<String, Object> originalFive = new HashMap<>();
        originalFive.put("Six", 456);

        Map<String, Object> originalFour = new HashMap<>();
        originalFour.put("Five", originalFive);

        Map<String, Object> originalOne = new HashMap<>();
        originalOne.put("One", 1);
        originalOne.put("Two", originalTwo);
        originalOne.put("Four", originalFour);

        String expectedString = "{\n"
                + "    \"One\": 1,\n"
                + "    \"Two\": {\n"
                + "        \"Three\": 23,\n"
                + "        \"Seven\": 27\n"
                + "    },\n"
                + "    \"Four\": {\n"
                + "        \"Five\": {\n"
                + "            \"Six\": 456\n"
                + "        }\n"
                + "    }\n"
                + "}";

        JSONObject expectedJson = new JSONObject(expectedString);

        JSONObject actualJson = SmapUpdater.convertMapsToJson(originalOne);

        assertEquals(expectedJson.toString(), actualJson.toString());
    }

    @Test
    public void convertTimeseriesToJsonTest() {
        Class clazz = Double.class;
        TimeSeries<Double> readings = new TimeSeries<>(clazz);
        readings.addReading(new Reading<>(Instant.ofEpochSecond(1454581652L), 5.0, clazz));
        readings.addReading(new Reading<>(Instant.ofEpochSecond(1462357652L), 15.0, clazz));
        readings.addReading(new Reading<>(Instant.ofEpochSecond(1462368452L), -55.0, clazz));

        String expectedString = "[\n"
                + "    [\n"
                + "        1454581652000,\n"
                + "        5.0\n"
                + "    ],\n"
                + "    [\n"
                + "        1462357652000,\n"
                + "        15.0\n"
                + "    ],\n"
                + "    [\n"
                + "        1462368452000,\n"
                + "        -55.0\n"
                + "    ]\n"
                + "]";

        JSONArray expectedJson = new JSONArray(expectedString);
        JSONArray actualJson = SmapUpdater.convertTimeseriesToJson(readings);

        assertEquals(expectedJson.toString(), actualJson.toString());
    }

    @Test
    public void generateUpdatePayloadTest() {
        String sourceName = "Prehistory";
        String path = "/path/to/stonehenge";
        Map<String, Object> metadataMap = new HashMap<>();
        metadataMap.put("Site", "Stonehenge");
        metadataMap.put("Type", "Temperature");
        metadataMap.put("SourceName", sourceName);
        metadataMap.put("Location/City", "Amesbury");
        metadataMap.put("Location/Latitude", "51.178889");
        metadataMap.put("Location/Longitude", "-1.825278");

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("Timezone", "Europe/London");
        propertiesMap.put("ReadingType", "double");
        propertiesMap.put("UnitofMeasure", "C");
        propertiesMap.put("UnitofTime", "ms");

        Class clazz = Double.class;
        TimeSeries<Double> readings = new TimeSeries<>(clazz);
        readings.addReading(new Reading<>(Instant.ofEpochSecond(1454581652L), 5.0, clazz));
        readings.addReading(new Reading<>(Instant.ofEpochSecond(1462357652L), 15.0, clazz));
        readings.addReading(new Reading<>(Instant.ofEpochSecond(1462368452L), -55.0, clazz));

        Payload actualPayload = SmapUpdater.generateUpdatePayload(
                sourceName,
                path,
                readings,
                Optional.of(metadataMap),
                Optional.of(propertiesMap),
                Optional.empty());

        String expectedString = "{\n"
                + "  \"uuid\": \"057fe03c-2579-3f8f-92cb-9b3fafe51723\",\n"
                + "  \"Metadata\": {\n"
                + "    \"Site\": \"Stonehenge\",\n"
                + "    \"Type\": \"Temperature\",\n"
                + "    \"SourceName\": \"Prehistory\",\n"
                + "    \"Location\": {\n"
                + "      \"City\": \"Amesbury\",\n"
                + "      \"Latitude\": \"51.178889\",\n"
                + "      \"Longitude\": \"-1.825278\"\n"
                + "    }\n"
                + "  },\n"
                + "  \"Properties\": {\n"
                + "    \"Timezone\": \"Europe/London\",\n"
                + "    \"ReadingType\": \"double\",\n"
                + "    \"UnitofMeasure\": \"C\",\n"
                + "    \"UnitofTime\": \"ms\"\n"
                + "  },\n"
                + "  \"Readings\": [\n"
                + "    [\n"
                + "      1454581652000,\n"
                + "      5.0\n"
                + "    ],\n"
                + "    [\n"
                + "      1462357652000,\n"
                + "      15.0\n"
                + "    ],\n"
                + "    [\n"
                + "      1462368452000,\n"
                + "      -55.0\n"
                + "    ]\n"
                + "  ]\n"
                + "}";

        JSONObject expectedJson = new JSONObject(expectedString);
        Payload expectedPayload = new Payload(path, expectedJson);

        assertEquals(expectedPayload, actualPayload);
    }

    @Test
    public void generateUpdatePayloadFromPropertiesTest() {
        String sourceName = "Prehistory";
        String columnName = "Stonehenge";
        String path = "/path/to/stonehenge";
        Map<String, Object> metadataMap = new HashMap<>();
        metadataMap.put("Site", "Stonehenge");
        metadataMap.put("Type", "Temperature");
        metadataMap.put("SourceName", sourceName);
        metadataMap.put("Location/City", "Amesbury");
        metadataMap.put("Location/Latitude", "51.178889");
        metadataMap.put("Location/Longitude", "-1.825278");

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("Timezone", "Europe/London");
        propertiesMap.put("ReadingType", "double");
        propertiesMap.put("UnitofMeasure", "C");
        propertiesMap.put("UnitofTime", "ms");

        Long[] timestampsArray = {1454581652L, 1462357652L, 1462368452L};
        Instant[] datetimesArray = {
            Instant.ofEpochSecond(1454581652L),
            Instant.ofEpochSecond(1462357652L),
            Instant.ofEpochSecond(1462368452L)
        };
        Double[] valuesArray = {5.0, 15.0, -55.0};
        
        Class clazz = Double.class;

        TimeSeries<Double> readings = new TimeSeries<>(clazz);
        for (int i = 0; i < timestampsArray.length; ++i) {
            readings.addReading(new Reading<>(datetimesArray[i], valuesArray[i], clazz));
        }

        Map<String, String> metadata = new HashMap<>();
        metadata.put("Properties/UnitofMeasure", "C");
        Measure measure = new Measure(columnName, clazz, metadata);
        DataFrame dataframe = new DataFrame(Arrays.asList(datetimesArray));
        dataframe.addColumn(measure, Arrays.asList(valuesArray));

        Properties outputProperties = new Properties();
        outputProperties.setProperty("Metadata/SourceName", sourceName);
        outputProperties.setProperty(columnName + "_PATH", path);
        outputProperties.setProperty(columnName + "_Metadata/Type", (String) metadataMap.get("Type"));
        outputProperties.setProperty("Metadata/Site", (String) metadataMap.get("Site"));
        outputProperties.setProperty("Metadata/Location/City", (String) metadataMap.get("Location/City"));
        outputProperties.setProperty("Metadata/Location/Latitude", (String) metadataMap.get("Location/Latitude"));
        outputProperties.setProperty("Metadata/Location/Longitude", (String) metadataMap.get("Location/Longitude"));
        outputProperties.setProperty("Properties/Timezone", (String) propertiesMap.get("Timezone"));
        outputProperties.setProperty("Properties/UnitofTime", (String) propertiesMap.get("UnitofTime"));

        JSONObject actualJson = SmapUpdater.generateUpdateJson(
                outputProperties, dataframe);

        String expectedString = "{\n"
                + "  \"/path/to/stonehenge\": {\n"
                + "    \"Metadata\": {\n"
                + "      \"Site\": \"Stonehenge\",\n"
                + "      \"Type\": \"Temperature\",\n"
                + "      \"SourceName\": \"Prehistory\",\n"
                + "      \"Location\": {\n"
                + "        \"Latitude\": \"51.178889\",\n"
                + "        \"City\": \"Amesbury\",\n"
                + "        \"Longitude\": \"-1.825278\"\n"
                + "      }\n"
                + "    },\n"
                + "    \"Readings\": [\n"
                + "      [\n"
                + "        1454581652000,\n"
                + "        5\n"
                + "      ],\n"
                + "      [\n"
                + "        1462357652000,\n"
                + "        15\n"
                + "      ],\n"
                + "      [\n"
                + "        1462368452000,\n"
                + "        -55\n"
                + "      ]\n"
                + "    ],\n"
                + "    \"Properties\": {\n"
                + "      \"Timezone\": \"Europe/London\",\n"
                + "      \"ReadingType\": \"double\",\n"
                + "      \"UnitofMeasure\": \"C\",\n"
                + "      \"UnitofTime\": \"ms\"\n"
                + "    },\n"
                + "    \"uuid\": \"057fe03c-2579-3f8f-92cb-9b3fafe51723\"\n"
                + "  }\n"
                + "}";

        JSONObject expectedJson = new JSONObject(expectedString);

        assertEquals(expectedJson.toString(), actualJson.toString());
    }
}
