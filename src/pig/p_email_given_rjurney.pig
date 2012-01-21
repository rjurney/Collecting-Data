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

sh rm -rf '/tmp/me_emails'; /* Workaround for PIG-2441 */
sh rm -rf '/tmp/num_emails';
sh rm -rf '/tmp/pair_counts';
sh rm -rf '/tmp/email_fractions';

messages = LOAD '/tmp/10000_emails.avro' USING AvroStorage();
messages = FILTER messages BY from IS NOT NULL AND to IS NOT NULL;

smaller = FOREACH messages GENERATE from, to;
with_cc = FOREACH messages GENERATE from, to, cc;

pairs = FOREACH smaller GENERATE from, FLATTEN(to) AS to:chararray;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

me = FILTER pairs BY from == 'russell.jurney@gmail.com';
-- STORE me INTO  '/tmp/me_emails';

/* Get the total number of emails */
all_groups = GROUP me ALL;
num_emails = FOREACH all_groups GENERATE COUNT(me) AS total;
-- STORE num_emails INTO '/tmp/num_emails';

/* Get the distribution of email addresses that we email */
by_pair = GROUP me BY (from, to);
pair_counts = FOREACH by_pair GENERATE FLATTEN(group) AS (from, to), SIZE(me) AS total;
STORE pair_counts INTO '/tmp/pair_counts';

email_fractions = FOREACH pair_counts GENERATE
        from, to,
        (double)((double)total / (double)num_emails.total) AS fraction:double;

STORE email_fractions INTO '/tmp/email_fractions';
