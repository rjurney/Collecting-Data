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

sh rm -rf '/tmp/email_from_to_cc_fractions' /* Workaround for PIG-2441 */

messages = LOAD '/tmp/10000_emails.avro' USING AvroStorage();
messages = FILTER messages BY from IS NOT NULL AND to IS NOT NULL;

/* Trim fields to get two-field totals, and lowercase */
smaller = FOREACH messages GENERATE from, to;
pairs = FOREACH smaller GENERATE from, FLATTEN(to) AS to:chararray;
pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

/* Get total counts of from/to pairs to use as divisor for from/to/cc pairs to get conditional probabilties. */
total_pairs = GROUP pairs BY (from, to);
pair_count = FOREACH total_pairs GENERATE FLATTEN(group) AS (from, to), COUNT(pairs.from) AS two_total;


/* To get cc counts, it must be present.  Then we lower it. */
with_cc = FOREACH messages GENERATE from, to, cc;
with_cc = FILTER with_cc BY cc IS NOT NULL;
ccs = FOREACH with_cc GENERATE from, FLATTEN(to) AS to, FLATTEN(cc) AS cc;

with_ccs = FILTER ccs BY cc IS NOT NULL;
three_pairs = GROUP with_ccs BY (from, to, cc);
three_count = FOREACH three_pairs GENERATE
    FLATTEN(group) AS (from, to, cc),
    COUNT(with_ccs.from) AS three_total;

/* Join three pairs with two pairs to get the total record in the three pair for division to get conditional probabilities. */
email_conditionals = JOIN three_count BY (from, to), pair_count BY (from, to);
email_conditionals = FOREACH email_conditionals GENERATE 
        three_count::from AS from,
        three_count::to AS to,
        three_count::cc AS cc,
        three_count::three_total AS three_total,
        pair_count::two_total AS two_total;

/* Now divide three_totals by two_totals to get the conditional probability. */
email_fractions = FOREACH email_conditionals GENERATE
        from, to, cc,
        (double)((double)three_total/(double)two_total) AS fraction:double;
        
me = FILTER email_fractions BY from == 'russell.jurney@gmail.com';

STORE me INTO '/tmp/email_from_to_cc_fractions';
