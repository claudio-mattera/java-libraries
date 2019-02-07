package dk.sdu.mmmi.cfei.smap;

/**
 * A metadata query for sMAP server.
 *
 * A query can be easily created using this helper class, and then fed to the
 * sMAP fetcher.
 *
 * <pre>
 * {@code
 * MetadataQuery query = MetadataQuery.valueOf("Metadata/Location/Room")
 *      .whereLike("path", "/path/to/room%")
 *      .whereIs("Metadata/SourceName", "Some source name");
 * SmapFetcher smapFetcher = new SmapFetcher(hostname, port, key);
 * smapFetcher.getRawMetadata(query);
 * }
 * </pre>
 *
 * @author cgim
 * @see SmapFetcher
 */
public class MetadataQuery {

    private MetadataQuery(String path, WhereClause whereClause) {
        this.path = path;
        this.whereClause = whereClause;
    }

    String getPath() {
        return this.path;
    }

    /**
     * MetadataQuery for a specific path.
     *
     * @param path The path.
     * @return A MetadataQuery object.
     */
    public static MetadataQuery valueOf(String path) {
        return new MetadataQuery(path, WhereClause.empty());
    }

    /**
     * Impose an equality constraint.
     *
     * E.g., retrieve only if "Path" is equal to "/some/path".
     *
     * Constraints clauses are joined with "and" logic.
     *
     * @param key The constraint field.
     * @param value The constraint value.
     * @return A MetadataQuery object
     */
    public MetadataQuery whereIs(String key, String value) {
        return new MetadataQuery(path, whereClause.whereIs(key, value));
    }

    /**
     * Impose an loose equality constraint.
     *
     * It uses the SQL-like operator '%'. E.g., retrieve only if "Path" is like
     * "/some/%/path".
     *
     * Constraints clauses are joined with "and" logic.
     *
     * @param key The constraint field.
     * @param value The constraint value.
     * @return A MetadataQuery object
     */
    public MetadataQuery whereLike(String key, String value) {
        return new MetadataQuery(path, whereClause.whereLike(key, value));
    }

    /**
     * Impose an tag constraint.
     *
     * Constraints clauses are joined with "and" logic.
     *
     * @param key The constraint field.
     * @param value The constraint value.
     * @return A MetadataQuery object
     */
    public MetadataQuery whereHas(String key, String value) {
        return new MetadataQuery(path, whereClause.whereHas(key, value));
    }

    /**
     * Impose a regular expression equality constraint.
     *
     * Constraints clauses are joined with "and" logic.
     *
     * @param key The constraint field.
     * @param value The constraint value.
     * @return A MetadataQuery object
     */
    public MetadataQuery whereRegexp(String key, String value) {
        return new MetadataQuery(path, whereClause.whereRegexp(key, value));
    }

    /**
     * Impose a raw constraints string.
     *
     * This can be used to impose constraint not supported by the current API.
     * E.g., it can be used to join clauses with "or" logic.
     *
     * @param raw The raw constraint string.
     * @return A MetadataQuery object
     */
    public MetadataQuery whereRaw(String raw) {
        return new MetadataQuery(path, whereClause.whereRaw(raw));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("select ");
        builder.append(path);
        String where = whereClause.toString();
        if (!where.isEmpty()) {
            builder.append(" where ");
            builder.append(where);
        }
        return builder.toString();
    }

    private final String path;
    private final WhereClause whereClause;
}
