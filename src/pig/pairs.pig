REGISTER /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
REGISTER /me/pig/contrib/piggybank/java/piggybank.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
sh rm -rf '/tmp/sent_counts.avro' /* Workaround for PIG-2441 */

messages = LOAD '/tmp/10000_emails.avro' USING AvroStorage();
messages = FILTER messages BY from IS NOT NULL AND to IS NOT NULL;
smaller = FOREACH messages GENERATE from, to;
pairs = FOREACH smaller GENERATE from, FLATTEN(to) AS to:chararray;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

froms = GROUP pairs BY (from, to);
sent_counts = FOREACH froms GENERATE FLATTEN(group) AS (from, to), SIZE(pairs) AS total;
STORE sent_counts INTO '/tmp/sent_counts.avro' USING AvroStorage();
