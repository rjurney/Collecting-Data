/* Load Avro jars and define shortcut */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

/* MongoDB */
register /me/mongo-hadoop/mongo-2.7.3.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0.jar
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

set default_parallel 5
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

/* Clean out old Mongo stores - sh calls don't work at the moment. */
/* 
sh mongo agile_data --quiet --eval 'db.ids_per_address.drop();'
sh mongo agile_data --quiet --eval 'db.addresses_per_id.drop();' 
*/

/* Load emails, filter null message_ids */
emails = load '/me/tmp/emails_big' using AvroStorage();
emails = filter emails by message_id IS NOT NULL;

/* Split out from/to/cc/bcc with message_id and then merge via UNION. */
senders = foreach emails generate FLATTEN(froms) as (real_name, address), message_id;
tos = foreach emails generate FLATTEN(tos) as (real_name, address), message_id;
ccs = foreach emails generate FLATTEN(ccs) as (real_name, address), message_id;
bccs = foreach emails generate FLATTEN(bccs) as (real_name, address), message_id;
email_address_messages = union senders, tos, ccs, bccs;

/* Just emails and message_ids for now, filter nulls */
email_address_messages = foreach email_address_messages generate address as email_address, message_id;
email_address_messages = filter email_address_messages by email_address IS NOT NULL AND email_address != '';

/* Package our ids for publishing to MongoDB, note the second step where we assign schema names to the group */
ids_per_address= group email_address_messages by email_address;
ids_per_address = foreach ids_per_address generate group as email_address, email_address_messages as email_address_messages;
addresses_per_id = group email_address_messages by message_id;
addresses_per_id = foreach addresses_per_id generate group as message_id, email_address_messages as email_address_messages;

/* Kepp collection real_names consistent with Pig relation real_names to avoid confusion. */
store ids_per_address into 'mongodb://localhost/agile_data.ids_per_address' using MongoStorage();
store addresses_per_id into 'mongodb://localhost/agile_data.addresses_per_id' using MongoStorage();
