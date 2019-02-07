sMAP for Java
====

This is a Java library to access data on an [sMAP] server. Check the Javadoc and tests for more examples.

<https://github.com/sdu-cfei/java-libraries/smap/>


Query objects
----

Query objects can be used to construct queries to sMAP. They are semantically equivalent to the sMAP SQL-like query language (in facts, they are converted to such queries when executed).

* Datetime selector
    - *in (start, end)*: returns all readings between dates *start* and *end*.
    - *before now*: returns all readings before the current date.
    - *before (end)*: returns all readings before date *end*.
    - *after (start)*: returns all readings after date *start*.
* limit $n$ selector: limits the number of returned readings to $n$.
* streamlimit $m$ selector: limits the number of returned streams to $m$.
* where *clause* selector: restricts the returned streams to the ones which satisfy *clause*.

Datetimes *start* and *end* can be expressed in either local time (Java class `java.time.LocalDateTime`) or Unix timestamps in milliseconds (Java class `java.time.Instant`).

Query objects support a list of where clauses that will be joined with *and* operator. In case it is necessary to join clauses with *or* operator, or with more complex logic (parenthesis, *or*, *not*...) it is possible to specify a *raw* clause, which will just be appended to the query.

### Examples

~~~~java
LocalDateTime start = LocalDateTime.of(2015, Month.MARCH, 12, 21, 15, 18);
LocalDateTime end = LocalDateTime.of(2015, Month.MARCH, 23, 14, 55, 2);
Query query = Query.in(start, end)
    .setLimit(2000)
    .setStreamLimit(12)
    .whereIs("Metadata/SourceName", "Some source name")
    .whereIs("Metadata/Description", "CO2 level")
    .whereLike("Metadata/RoomName", "ABC 123 %");

query.toString();
// Returns "select data in ('2015-03-12 21:15:18', '2015-03-23 14:55:02')
//          limit 2000 streamlimit 12 where
//          Metadata/SourceName = 'Some source name' and
//          Metadata/Description = 'CO2 level' and
//          Metadata/RoomName like 'ABC 123 %'"
// (without newlines)
~~~~

~~~~java
Instant end = LocalDateTime.of(2015, Month.MARCH, 23, 14, 55, 2)
    .toInstant(ZoneOffset.UTC);
Query query = Query.before(end)
    .setStreamLimit(12)
    .whereRaw("Metadata/SourceName = 'Some source name' or Metadata/Path like '/Some/Path/%'");

query.toString();
// Returns "select data before 1427118902000
//          streamlimit 12 where
//          Metadata/SourceName = 'Some source name' or
//          Metadata/Path like '/Some/Path/%'"
// (without newlines)
~~~~


Retrieving data
----

Call `SmapFetcher.execute()` to retrieve readings from a sMAP server:

~~~~java
SmapFetcher fetcher = SmapFetcher(host, port);
Query query = ...

Map<String, TimeSeries> results = fetcher.execute(query);
// Returns a map UUID -> time series
~~~~


Posting new data
----

In order to post new data to sMAP, first it is necessary to generate an update payload, and then to post it.

In the following code, new data is posted to a single stream.

~~~~java
Payload payload = generateUpdatePayload(
            sourceName,    // String
            path,          // String
            readings,      // TimeSeries<Double>
            metadataOpt,   // Optional<Map<String, Object>>
            propertiesOpt, // Optional<Map<String, Object>>
            suppliedUuid); // Optional<UUID>

SmapUpdater updater = SmapUpdater(host, port, key, dryRun);

Map map = new HashMap();
map.put(payload.path, payload.object);

JSONObject object = new JSONObject(map)

updater.postData(object);
~~~~

In the following code, new data is posted to multiple streams.

~~~~java
List<Payload> payloads = ...

SmapUpdater updater = SmapUpdater(host, port, key, dryRun);

JSONObject cumulativePayload = new JSONObject(
        payloads
        .stream()
        .collect(Collectors.
                toMap(pair -> pair.path, pair -> pair.object)));

updater.postData(cumulativePayload);
~~~~

### Auto-generating UUID

When creating an update payload for a given source name and path, it is possible to supply the corresponding UUID or to automatically generate one.
In the latter case, the UUID corresponds to `MD5(source_name ++ path)`.

**Note**: this functionality is present for legacy reasons, the UUID should always be specified explicitly.


Installation
----

This library uses Maven and can be installed with the following command.

~~~~bash
mvn install
~~~~

EPW files are CSV files with an additional header, therefore, this library used the [DataFrames](https://github.com/sdu-cfei/java-libraries/dataframes/) library for managing the raw data.


Documentation
----

[Javadoc](https://sdu-cfei.github.io/java-libraries/smap-docs/index.html)



[sMAP]: http://people.eecs.berkeley.edu/~stevedh/smap2/index.html
