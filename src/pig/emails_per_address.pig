/* Load ElasticSearch integration */
register /me/wonderdog/target/wonderdog-1.0-SNAPSHOT.jar;
register /me/elasticsearch-0.18.6/lib/elasticsearch-0.18.6.jar;
register /me/elasticsearch-0.18.6/lib/jline-0.9.94.jar;
register /me/elasticsearch-0.18.6/lib/jna-3.2.7.jar;
register /me/elasticsearch-0.18.6/lib/log4j-1.2.16.jar;
register /me/elasticsearch-0.18.6/lib/lucene-analyzers-3.5.0.jar;
register /me/elasticsearch-0.18.6/lib/lucene-core-3.5.0.jar;
register /me/elasticsearch-0.18.6/lib/lucene-highlighter-3.5.0.jar;
register /me/elasticsearch-0.18.6/lib/lucene-memory-3.5.0.jar;
register /me/elasticsearch-0.18.6/lib/lucene-queries-3.5.0.jar;

/* Load Avro jars */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

/* MongoDB */
register /me/mongo-hadoop/mongo-2.7.2.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0-rc0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0-rc0.jar

set default_parallel 5
set pig.piggybank.storage.avro.bad.record.threshold 1.0
set pig.piggybank.storage.avro.bad.record.min 5000
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

/* Define aliases for long UDFs */
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();
define ElasticSearch com.infochimps.elasticsearch.pig.ElasticSearchStorage('/me/elasticsearch-0.18.6/config/elasticsearch.yml', '/me/elasticsearch-0.18.6/plugins');

/* Filter emails according to existence of header pairs: [from, to, cc, bcc, reply_to]
Then project the header part, message_id and subject, and emit them, lowercased. */
DEFINE headers_messages(email, col) RETURNS set { 
  filtered = FILTER $email BY ($col IS NOT NULL);
  flat = FOREACH filtered GENERATE FLATTEN($col) AS $col, message_id, subject, date;
  lowered = FOREACH flat GENERATE LOWER($col) AS address, message_id, subject, date;
  $set = FILTER lowered BY (address IS NOT NULL) and (address != '') and (date IS NOT NULL);
}

/* Nuke the email/address index, as we are about to replace it. */
sh curl -XDELETE 'http://localhost:9200/address/emails'
/* Nuke the Mongo store, as we are about to replace it. */
-- sh mongo agile_data --eval 'db.emails_per_address.drop\(\)'

rmf /tmp/emails_per_address.json

emails = load '/me/tmp/emails' using AvroStorage();
froms = headers_messages(emails, 'from');
tos = headers_messages(emails, 'to');
ccs = headers_messages(emails, 'cc');
bccs = headers_messages(emails, 'bcc');
reply_tos = headers_messages(emails, 'reply_to');

address_messages = UNION froms, tos, ccs, bccs, reply_tos;

emails_per_address = group address_messages by address;
emails_per_address = foreach emails_per_address { address_messages = order address_messages by date desc;
                                                  generate group as address, 
                                                           address_messages as address_messages; }

store emails_per_address into 'mongodb://localhost/agile_data.emails_per_address' using MongoStorage();

/* Jsonify, store and load to index in ElasticSearch as JSON */
store emails_per_address into '/tmp/emails_per_address.json' using JsonStorage();
emails_per_address = load '/tmp/emails_per_address.json' as (json_record:chararray);
store emails_per_address into 'es://address/emails?id=address&json=true&size=1000' using ElasticSearch();

/* Verify that we get a record */
sh curl -XGET 'http://localhost:9200/address/emails/_search?q=hadoop&pretty=true&size=1'

/* Now, for example: curl -XGET 'http://localhost:9200/address/emails/_search?q=hadoop&pretty=true&size=1' 
   will return the top hit about hadoop.  Woohoo!  */
