package dk.sdu.mmmi.cfei.smap;

import java.util.Objects;
import org.json.JSONObject;

/**
 * A value object to represent an update payload for a sMAP server.
 *
 * An update payload consists of a path and a corresponding JSON value, e.g.:
 *
 * <pre>
 * {@code
 * {
 *  "uuid": "abcdef01-abcd-ef01-abcd-0123456789ab",
 *  "Metadata": {
 *    "Site": "Stonehenge",
 *    "Type": "Temperature",
 *    "SourceName": "Prehistory",
 *    "Location": {
 *      "City": "Amesbury",
 *      "Latitude": "51.178889",
 *      "Longitude": "-1.825278"
 *    }
 *  },
 *  "Properties": {
 *    "Timezone": "Europe/London",
 *    "ReadingType": "double",
 *    "UnitofMeasure": "C",
 *    "UnitofTime": "ms"
 *  },
 *  "Readings": [
 *    [
 *      1454581652000,
 *      5.0
 *    ],
 *    [
 *      1462357652000,
 *      15.0
 *    ],
 *    [
 *      1462368452000,
 *      -55.0
 *    ]
 *  ]
 * }
 * }
 * </pre>
 *
 * @author cgim
 * @see SmapUpdater
 */
public class Payload {

    /**
     * Creates a payload.
     *
     * @param path The path.
     * @param object The JSON value.
     */
    public Payload(String path, JSONObject object) {
        this.path = path;
        this.object = object;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 31 * hash + Objects.hashCode(this.path);
        hash = 31 * hash + Objects.hashCode(this.object);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Payload other = (Payload) obj;
        if (!Objects.equals(this.path, other.path)) {
            return false;
        }
        return this.object.toString().equals(other.object.toString());
    }

    public final String path;
    public final JSONObject object;
}
