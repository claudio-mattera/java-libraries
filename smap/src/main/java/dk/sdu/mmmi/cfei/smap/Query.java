package dk.sdu.mmmi.cfei.smap;

import fj.data.Either;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * A query for sMAP server.
 *
 * A query can be easily created using this helper class, and then fed to the
 * sMAP fetcher.
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
 * @see SmapFetcher
 */
public class Query {

    private Query(
            SelectorType data,
            Either<Instant, LocalDateTime> start,
            Either<Instant, LocalDateTime> end,
            Optional<Long> limit,
            Optional<Long> streamLimit,
            WhereClause whereClause) {
        this.selectorType = data;
        this.start = start;
        this.end = end;
        this.limit = limit;
        this.streamLimit = streamLimit;
        this.whereClause = whereClause;
    }

    private Query(
            SelectorType data,
            Either<Instant, LocalDateTime> start,
            Either<Instant, LocalDateTime> end,
            Optional<Long> limit,
            Optional<Long> streamLimit) {
        this.selectorType = data;
        this.start = start;
        this.end = end;
        this.limit = limit;
        this.streamLimit = streamLimit;
        this.whereClause = WhereClause.empty();
    }

    private Query(
            SelectorType data,
            Either<Instant, LocalDateTime> start,
            Either<Instant, LocalDateTime> end) {
        this.selectorType = data;
        this.start = start;
        this.end = end;
        this.limit = Optional.empty();
        this.streamLimit = Optional.empty();
        this.whereClause = WhereClause.empty();
    }

    /**
     * Query for readings following a reference date.
     *
     * @param ref The reference date.
     * @return A query object.
     */
    public static Query after(LocalDateTime ref) {
        return new Query(
                SelectorType.AFTER,
                Either.right(ref),
                Either.right(LocalDateTime.MAX)
        );
    }

    /**
     * Query for readings following a reference date.
     *
     * @param ref The reference date.
     * @return A query object.
     */
    public static Query after(Instant ref) {
        return new Query(
                SelectorType.AFTER,
                Either.left(ref),
                Either.left(Instant.MAX)
        );
    }

    /**
     * Query for readings preceding a reference date.
     *
     * @param ref The reference date.
     * @return A query object.
     */
    public static Query before(LocalDateTime ref) {
        return new Query(
                SelectorType.BEFORE,
                Either.right(LocalDateTime.MIN),
                Either.right(ref)
        );
    }

    /**
     * Query for readings preceding a reference date.
     *
     * @param ref The reference date.
     * @return A query object.
     */
    public static Query before(Instant ref) {
        return new Query(
                SelectorType.BEFORE,
                Either.left(Instant.MIN),
                Either.left(ref)
        );
    }

    /**
     * Query for readings preceding the current date.
     *
     * This useful when also setting a limit, e.g., retrieve the last N
     * readings.
     *
     * @return A query object.
     */
    public static Query beforeNow() {
        return new Query(
                SelectorType.BEFORE_NOW,
                Either.left(Instant.MIN),
                Either.left(Instant.MAX)
        );
    }

    /**
     * Query for readings between two reference dates.
     *
     * @param start The starting date.
     * @param end The ending date.
     * @return A query object.
     */
    public static Query in(LocalDateTime start, LocalDateTime end) {
        return new Query(
                SelectorType.IN,
                Either.right(start),
                Either.right(end)
        );
    }

    /**
     * Query for readings between two reference dates.
     *
     * @param start The starting date.
     * @param end The ending date.
     * @return A query object.
     */
    public static Query in(Instant start, Instant end) {
        return new Query(
                SelectorType.IN,
                Either.left(start),
                Either.left(end)
        );
    }

    /**
     * Limit the number of readings retrieved.
     *
     * @param limit The maximal number of readings.
     * @return A query object
     */
    public Query setLimit(long limit) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                Optional.of(limit),
                this.streamLimit
        );
    }

    /**
     * Limit the number of streams retrieved.
     *
     * @param streamLimit The maximal number of streams.
     * @return A query object
     */
    public Query setStreamLimit(long streamLimit) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                this.limit,
                Optional.of(streamLimit)
        );
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
     * @return A query object
     */
    public Query whereIs(String key, String value) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                this.limit,
                this.streamLimit,
                whereClause.whereIs(key, value)
        );
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
     * @return A query object
     */
    public Query whereLike(String key, String value) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                this.limit,
                this.streamLimit,
                whereClause.whereLike(key, value)
        );
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
    public Query whereHas(String key, String value) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                this.limit,
                this.streamLimit,
                whereClause.whereHas(key, value)
        );
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
    public Query whereRegexp(String key, String value) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                this.limit,
                this.streamLimit,
                whereClause.whereRegexp(key, value)
        );
    }

    /**
     * Impose a raw constraints string.
     *
     * This can be used to impose constraint not supported by the current API.
     * E.g., it can be used to join clauses with "or" logic.
     *
     * @param raw The raw constraint string.
     * @return A query object
     */
    public Query whereRaw(String raw) {
        return new Query(
                this.selectorType,
                this.start,
                this.end,
                this.limit,
                this.streamLimit,
                whereClause.whereRaw(raw)
        );
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("select data ");
        switch (selectorType) {
            case AFTER:
                builder.append("after ");
                builder.append(eitherToString(start));
                break;
            case BEFORE:
                builder.append("before ");
                builder.append(eitherToString(end));
                break;
            case BEFORE_NOW:
                builder.append("before now");
                break;
            case IN:
                builder.append("in (");
                builder.append(eitherToString(start));
                builder.append(", ");
                builder.append(eitherToString(end));
                builder.append(")");
                break;
        }
        limit.ifPresent(value -> {
            builder.append(" limit ");
            builder.append(value);
        });
        streamLimit.ifPresent(value -> {
            builder.append(" streamlimit ");
            builder.append(value);
        });
        String where = whereClause.toString();
        if (!where.isEmpty()) {
            builder.append(" where ");
            builder.append(where);
        }
        return builder.toString();
    }

    private static String eitherToString(Either<Instant, LocalDateTime> value) {
        return value.either(
                instant -> String.valueOf(instant.toEpochMilli()),
                datetime -> {
                    StringBuilder builder = new StringBuilder();
                    builder.append("'");
                    builder.append(datetime.format(FORMATTER));
                    builder.append("'");
                    return builder.toString();
                });
    }

    private final SelectorType selectorType;
    private final Either<Instant, LocalDateTime> start;
    private final Either<Instant, LocalDateTime> end;
    private final Optional<Long> limit;
    private final Optional<Long> streamLimit;
    private final WhereClause whereClause;

    private enum SelectorType {
        AFTER,
        BEFORE,
        BEFORE_NOW,
        IN
    }
    private static final DateTimeFormatter FORMATTER
            = DateTimeFormatter.ofPattern("MM/dd/YYYY HH:mm");
}
