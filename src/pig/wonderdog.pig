/* Load ElasticSearch integration and define shortcut*/
register /me/wonderdog/target/wonderdog*.jar;
register /me/elasticsearch-0.18.6/lib/*.jar; /* */

%default es.config '/me/elasticsearch-0.18.6/config/elasticsearch.yml'
%default es.path.plugins '/me/elasticsearch-0.18.6/plugins'

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

/* Nuke the elasticsearch email index, as we are about to replace it. */
sh curl -XDELETE 'http://localhost:9200/email/email'
/*rmf /tmp/emails.json

emails = load '/me/tmp/emails_big/part-1.avro' using AvroStorage();
emails = filter emails by message_id IS NOT NULL;
*/
/*store emails into '/tmp/emails.json' using JsonStorage();
rm /tmp/emails.json/.pig_header 
rm /tmp/emails.json/.pig_schema 

store emails into 'mongodb://localhost/agile_data.emails' using MongoStorage();
*/
json_emails = load '/tmp/emails.json' AS (json_record:chararray);
store json_emails into 'es://email/email?id=message_id&json=true&size=1000' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage('/me/elasticsearch-0.18.6/config/elasticsearch.yml', '/me/elasticsearch-0.18.6/plugins');
sh curl -XGET 'http://localhost:9200/email/email/_search?q=hadoop&pretty=true&size=1'

/* Now, for example: curl -XGET 'http://localhost:9200/email/email/_search?q=hadoop&pretty=true&size=1' 
   will return the top hit about hadoop.  Woohoo!  */
