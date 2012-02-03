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

rmf /tmp/social_network_edge_list.txt

/* Filter emails according to existence of header pairs, from and [to, cc, bcc]
project the pairs (may be more than one to/cc/bcc), then emit them, lowercased. */
DEFINE header_pairs(email, col1, col2) RETURNS pairs { 
  filtered = FILTER $email BY ($col1 IS NOT NULL) AND ($col2 IS NOT NULL);
  flat = FOREACH filtered GENERATE FLATTEN($col1) AS $col1, FLATTEN($col2) AS $col2;
  $pairs = FOREACH flat GENERATE LOWER($col1) AS ego1, LOWER($col2) AS ego2;
}

/* Get email address pairs for each type of connection, and union them together */
emails = LOAD '/tmp/russell.jurney.gmail.com.avro' USING AvroStorage();
from_to = header_pairs(emails, from, to);
from_cc = header_pairs(emails, from, cc);
from_bcc = header_pairs(emails, from, bcc);
pairs = UNION from_to, from_cc, from_bcc;

/* Get a count of emails over these edges. */
pair_groups = GROUP pairs BY (ego1, ego2);
sent_counts = FOREACH pair_groups GENERATE FLATTEN(group) AS (ego1, ego2), COUNT_STAR(pairs) AS total;

STORE sent_counts INTO '/tmp/social_network_edge_list.txt';/* USING AvroStorage();
STORE sent_counts INTO 'mongodb://localhost/test.pig' USING MongoStorage(); */
