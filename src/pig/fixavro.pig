/* Load Avro jars */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

/* MongoDB */
register /me/mongo-hadoop/mongo-2.7.2.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0-rc0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0-rc0.jar

/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

/* Pig configuration */
set default_parallel 5
set pig.piggybank.storage.avro.bad.record.threshold 1.0
set pig.piggybank.storage.avro.bad.record.min 5000
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

/* Shortcuts */
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

/* Always remove this job's output */
rmf /tmp/froms.json
rmf /tmp/froms.pig
rmf /tmp/froms.avro

/* We want maximum debug output. Also run with -v -w */
set warn DEBUG

emails = load '/me/tmp/emails' using AvroStorage();
f = foreach emails generate message_id, from, to;
f = filter emails by SIZE(to) > 1;
g = foreach f generate message_id, 
                       (bag{tuple(chararray)})from as froms:bag{tuple(from:chararray)},
                       (bag{tuple(chararray)})to as tos:bag{tuple(to:chararray)};

store g into '/tmp/froms.pig';
store g into '/tmp/froms.json' using JsonStorage();
store g into '/tmp/froms.avro' using AvroStorage();
store g into sfag using MongoStorage();