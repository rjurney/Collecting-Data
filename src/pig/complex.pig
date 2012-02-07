REGISTER /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
REGISTER /me/pig/contrib/piggybank/java/piggybank.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
REGISTER /me/mongo-hadoop/mongo-2.3.jar
REGISTER /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0-SNAPSHOT.jar
REGISTER /me/mongo-hadoop/pig/target/mongo-pig-1.0-SNAPSHOT.jar

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
DEFINE MongoStorage com.mongodb.hadoop.pig.MongoStorage();

rmf '/tmp/sent_counts.avro' /* Workaround for PIG-2441 */

messages = LOAD '/me/tmp/test_inbox' USING AvroStorage();
messages = FILTER messages BY from IS NOT NULL AND to IS NOT NULL;

smaller = FOREACH messages GENERATE from, to, subject;
pairs = FOREACH smaller GENERATE FLATTEN(from) as from, FLATTEN(to) AS to, subject;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to, subject;

froms = GROUP pairs BY (from, to);

sent_topics = FOREACH forms GENERATE FLATTEN(group) AS (from, to)
                                     pairs.subject as subject;
/*sent_topics = FOREACH froms GENERATE FLATTEN(group) AS (from, to), 
                                     pairs.subject AS pairs:bag {column:tuple (subject:chararray)};
*/
STORE sent_topics INTO 'mongodb://localhost/test.pigola2' USING MongoStorage();
