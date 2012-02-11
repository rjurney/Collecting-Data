register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/mongo-hadoop/mongo-2.3.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0-SNAPSHOT.jar
register /me/mongo-hadoop/pig/target/mongo-pig-1.0-SNAPSHOT.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

import 'macros.pig';
set aggregate.warning 'true';

rmf /tmp/top_friends.avro

emails = load '/me/tmp/again_inbox' using AvroStorage();

emails = filter emails by (from is not null);
tos = foreach emails generate flatten(from) as from, flatten(to) as to;
ccs = foreach emails generate flatten(from) as from, flatten(cc) as cc;
pairs = union tos, ccs;
counts = foreach (group pairs by (from, to)) generate flatten(group) as (from, to), COUNT(pairs) as total;

top_pairs = foreach (group counts by from) {
  filtered = filter counts by (to is not null);
  sorted = order filtered by total desc;
  top_20 = limit sorted 30;
  generate group as email, top_20.(to) as top_20;
}

store top_pairs into '/tmp/top_friends.avro' using AvroStorage();
store top_pairs into 'mongodb://localhost/agile_data.top_friends' using MongoStorage();
