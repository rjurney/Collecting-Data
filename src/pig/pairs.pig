REGISTER /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
REGISTER /me/pig/contrib/piggybank/java/piggybank.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

rmf /tmp/sent_counts

-- messages = LOAD 's3n://agile.data/again_inbox' USING AvroStorage();
messages = LOAD 'file:///me/tmp/again_inbox' USING AvroStorage();
messages = FILTER messages BY (from IS NOT NULL) AND (to IS NOT NULL);
smaller = FOREACH messages GENERATE from, to;
pairs = FOREACH smaller GENERATE FLATTEN(from) as from, FLATTEN(to) AS to;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

froms = GROUP pairs BY (from, to);
sent_counts = FOREACH froms GENERATE group AS (from, to), COUNT(pairs) AS total;
-- STORE sent_counts INTO 's3n://agile.data/sent_counts' USING AvroStorage();
STORE sent_counts INTO '/tmp/sent_counts' USING AvroStorage();
