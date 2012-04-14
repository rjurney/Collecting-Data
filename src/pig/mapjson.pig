register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

define MapToJson org.apache.pig.piggybank.evaluation.json.MapToJson();
rmf /tmp/json.txt

maps = LOAD '/tmp/map.txt' AS (M:map []);
test = FOREACH maps GENERATE MapToJson(M) AS json:chararray;

store test INTO '/tmp/json.txt';