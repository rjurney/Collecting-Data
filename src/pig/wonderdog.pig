/* Load ElasticSearch integration */
register '/me/wonderdog/target/wonderdog-1.0-SNAPSHOT.jar';
register '/me/elasticsearch-0.18.6/lib/elasticsearch-0.18.6.jar';
register '/me/elasticsearch-0.18.6/lib/jline-0.9.94.jar';
register '/me/elasticsearch-0.18.6/lib/jna-3.2.7.jar';
register '/me/elasticsearch-0.18.6/lib/log4j-1.2.16.jar';
register '/me/elasticsearch-0.18.6/lib/lucene-analyzers-3.5.0.jar';
register '/me/elasticsearch-0.18.6/lib/lucene-core-3.5.0.jar';
register '/me/elasticsearch-0.18.6/lib/lucene-highlighter-3.5.0.jar';
register '/me/elasticsearch-0.18.6/lib/lucene-memory-3.5.0.jar';
register '/me/elasticsearch-0.18.6/lib/lucene-queries-3.5.0.jar';

/* Load Avro jars */
register /me/newpig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/newpig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/newpig/contrib/piggybank/java/piggybank.jar
register /me/newpig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/newpig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/newpig/build/ivy/lib/Pig/joda-time-1.6.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
-- define ElasticSearch com.infochimps.elasticsearch.pig.ElasticSearchStorage();

messages = load '/me/tmp/emails.avro' using AvroStorage();
messages = FILTER messages BY (from IS NOT NULL) AND (to IS NOT NULL);
smaller = FOREACH messages GENERATE FLATTEN(from) as from, FLATTEN(to) as to;
pairs = FOREACH smaller GENERATE LOWER(from) AS from, LOWER(to) AS to;

froms = GROUP pairs BY (from, to) PARALLEL 10;
sent_counts = FOREACH froms GENERATE FLATTEN(group) AS (from, to), COUNT(pairs) AS total;

STORE sent_counts INTO '/tmp/sent_counts';  

/*STORE sent_counts INTO 'es://sent_counts/sent_counts?json=false&size=1000' USING 
  com.infochimps.elasticsearch.pig.ElasticSearchStorage('/me/elasticsearch-0.18.6/config/elasticsearch.yml', '/me/elasticsearch-0.18.6/plugins');
*/