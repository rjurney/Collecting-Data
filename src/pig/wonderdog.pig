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
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

/* MongoDB */
register /me/mongo-hadoop/mongo-2.7.2.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0-rc0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0-rc0.jar

set default_parallel 5
set pig.piggybank.storage.avro.bad.record.threshold 0.01
set pig.piggybank.storage.avro.bad.record.min 100
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();
define ElasticSearch com.infochimps.elasticsearch.pig.ElasticSearchStorage('/me/elasticsearch-0.18.6/config/elasticsearch.yml', '/me/elasticsearch-0.18.6/plugins');

emails = load '/me/tmp/emails' using AvroStorage();

store emails into 'mongodb://localhost/agile_data.emails' using MongoStorage();
store emails into 'es://email/email?json=false&size=1000' using ElasticSearch();

/* Now, for example: curl -XGET 'http://localhost:9200/email/email/_search?q=hadoop&pretty=true&size=1' 
   will return the top hit about hadoop.  Woohoo!  */
