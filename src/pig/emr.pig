register $pigdir/build/ivy/lib/Pig/avro-1.5.3.jar
register $pigdir/build/ivy/lib/Pig/json-simple-1.1.jar
register $pigdir/contrib/piggybank/java/piggybank.jar
register $pigdir/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register $pigdir/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register $pigdir/build/ivy/lib/Pig/joda-time-1.6.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

messages = load '$input' using AvroStorage();
messages = FILTER messages BY (from IS NOT NULL) AND (to IS NOT NULL);
smaller = FOREACH messages GENERATE from, to;
pairs = FOREACH smaller GENERATE FLATTEN(from) as from, FLATTEN(to) AS to;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

froms = GROUP pairs BY (from, to) PARALLEL 10;
sent_counts = FOREACH froms GENERATE FLATTEN(group) AS (from, to), COUNT(pairs) AS total;

store sent_counts INTO '$output' USING AvroStorage();