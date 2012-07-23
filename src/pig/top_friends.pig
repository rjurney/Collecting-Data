register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

set aggregate.warning 'true';

rmf /tmp/top_friends.avro

avro_emails = load '/me/tmp/thu_emails' using AvroStorage();
avro_emails = filter avro_emails by (froms is not null);

/* We need to insert reply_to as a valid from or email addresses will miss in our index */
split avro_emails into has_reply_to if (reply_tos is not null), just_froms if (reply_tos is null);

/* Count both the from and reply_to as valid froms if there is a reply_tos field */
reply_tos = foreach has_reply_to generate reply_tos as froms, tos, ccs, bccs;
reply_to_froms = foreach has_reply_to generate froms, tos, ccs, bccs;
/* Treat emails without reply_to as normal */
just_froms = foreach just_froms generate froms, tos, ccs, bccs;
/* Now union them all and we have our dataset to compute on */
emails = union reply_tos, reply_to_froms, just_froms;

/* Now pair up our froms/reply_tos with all recipient types, 
   and union them to get a sender/recipient connection list. */
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
