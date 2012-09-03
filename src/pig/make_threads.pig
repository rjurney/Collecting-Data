/* AvroStorage */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

/* MongoStorage */
register /me/mongo-hadoop/mongo-2.7.3.jar
/*register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0.jar*/
register /me/tmp/mongo-hadoop/core/target/mongo-hadoop-core-1.1.0-SNAPSHOT.jar
register /me/tmp/mongo-hadoop/pig/target/mongo-hadoop-pig-1.1.0-SNAPSHOT.jar
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

-- rmf /tmp/threads.avro

/* Get rid of emails with reply_to, as they confuse everything in mailing lists. */
avro_emails = load '/me/tmp/thu_emails' using AvroStorage();
emails = filter avro_emails by (froms is not null);

email_threads = foreach (group emails by thread_id) {
  thread = order emails by date;
  generate group as thread_id, thread;
};

email_threads = foreach email_threads generate thread_id, thread as thread:bag{email:tuple(message_id:chararray, thread_id:chararray, in_reply_to:chararray, subject:chararray, body:chararray, date:chararray, froms:bag{from:tuple(real_name:chararray, address:chararray)}, tos:bag{to:tuple(real_name:chararray, address:chararray)}, ccs:bag{cc:tuple(real_name:chararray, address:chararray)}, bccs:bag{bcc:tuple(real_name:chararray, address:chararray)}, reply_tos:bag{reply_to:tuple(real_name:chararray, address:chararray)})};
-- email_threads = load '/tmp/threads.avro' as thread:bag{email:tuple(message_id:chararray, thread_id:chararray, in_reply_to:chararray, subject:chararray, body:chararray, date:chararray, froms:bag{from:tuple(real_name:chararray, address:chararray)}, tos:bag{to:tuple(real_name:chararray, address:chararray)}, ccs:bag{cc:tuple(real_name:chararray, address:chararray)}, bccs:bag{bcc:tuple(real_name:chararray, address:chararray)}, reply_tos:bag{reply_to:tuple(real_name:chararray, address:chararray)})};
store email_threads into 'mongodb://localhost/agile_data.threads' using MongoStorage();
-- store email_threads into '/tmp/threads.avro' using AvroStorage();
-- '{thread_id : 1},{unique : true}');
