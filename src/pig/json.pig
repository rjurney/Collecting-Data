register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MapToJson org.apache.pig.piggybank.evaluation.json.MapToJson();

rmf /tmp/json.txt

messages = load '/me/tmp/emails' using AvroStorage();
messages = FILTER messages BY (from IS NOT NULL) AND (to IS NOT NULL);
smaller = FOREACH messages GENERATE from, to;
pairs = FOREACH smaller GENERATE FLATTEN(from) as from, FLATTEN(to) AS to;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

froms = GROUP pairs BY (from, to);
STORE froms INTO '/tmp/json.txt' USING JsonStorage();
/*test = foreach froms GENERATE FLATTEN(group) AS (from, to), MapToJson(pairs) as JsonText:chararray;

store test INTO '/tmp/json.txt';*/