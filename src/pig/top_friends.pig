register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

set aggregate.warning 'true';

rmf /tmp/top_friends.avro

emails = load '/me/tmp/thu_emails' using AvroStorage();

emails = filter emails by (froms is not null);
tos = foreach emails generate flatten(froms.address) as from, flatten(tos.address) as to;
ccs = foreach emails generate flatten(froms.address) as from, flatten(ccs.address) as to;
bccs = foreach emails generate flatten(froms.address) as from, flatten(bccs.address) as to;
pairs = union tos, ccs, bccs;
counts = foreach (group pairs by (from, to)) generate flatten(group) as (from, to), COUNT(pairs) as total;

top_pairs = foreach (group counts by from) {
  filtered = filter counts by (to is not null);
  sorted = order filtered by total desc;
  top_20 = limit sorted 30;
  generate group as email, top_20.(to) as top_20;
}

store top_pairs into '/tmp/top_friends.avro' using AvroStorage();
