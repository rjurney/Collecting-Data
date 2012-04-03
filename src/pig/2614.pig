/* Load Avro jars */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

set default_parallel 5
/*set pig.piggybank.storage.avro.bad.record.threshold 1.0
set pig.piggybank.storage.avro.bad.record.min 1000000*/

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

rmf /tmp/bodies

emails = load '/me/tmp/test_inbox' using AvroStorage();
s = foreach emails generate subject, body;
STORE s into '/tmp/bodies' using AvroStorage();
