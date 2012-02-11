register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/mongo-hadoop/mongo-2.3.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0-SNAPSHOT.jar
register /me/mongo-hadoop/pig/target/mongo-pig-1.0-SNAPSHOT.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

import 'macros.pig';
set aggregate.warning 'true';
set default_parallel 3;

rmf /tmp/ngrams.avro

-- Load the emails
emails = load '/me/tmp/again_inbox' using AvroStorage();

/* TODO: bring in in_reply_to also to calculate for mailing lists */
-- trim fields to the from and the message body
bodies = foreach emails generate flatten(from) as from, body;

-- ensure all records have from and body to avoid errors in further processing
bodies = filter bodies by (body is not null) and (from is not null);

-- get rid of carriage returns, relpace them with spaces
no_cr = foreach bodies generate LOWER(from) as from, REPLACE(body, '\\n', ' ') as body;

-- Split the body into words in a bag, and then flatten that bag to create many one-word records
chunks = foreach no_cr generate from, flatten(TOKENIZE(LOWER(body))) as word;

-- Ensure there are alphanumeric characters so that these entries are words.  We can do more here.
words = filter chunks by (word matches '\\w+[^_\\W]');
-- 

-- Trim fields down to just words, since this is a global list and not a per-from list
all_words = foreach words generate from, word;
-- Distinct gets us document ocurrance count per word
per_document = distinct all_words;
-- Document frequency is then derived
document_frequency = foreach (group per_document by word) generate group as word,
                                                                   (int)COUNT(per_document) as document_total;

-- Now group words by from and get a word count per email
term_count = foreach (group words by (from, word)) generate flatten(group) as (from, word),
                                                            (int)COUNT(words) as term_total;

-- Get a word count per document to normalize term_cout
doc_word_count = foreach (group words by from) generate group as from, COUNT(words) as word_total;

-- Normalize the term_frequency by the word count for that document
normalize_join = join term_count by from, doc_word_count by from;
term_frequency = foreach normalize_join generate term_count::from as from,
                                                 term_count::word as word,
                                                 (double)term_total/(double)word_total as term_total;

-- bring the term and document frequencies together 
together = join term_frequency BY word, document_frequency by word;
tf_idf_scores = foreach together generate from as from, 
                                   term_frequency::word as word, 
                                   (double)term_total/(double)document_total as score;

-- For each message_id, store the top 100 scores per from
top_30 = foreach (group tf_idf_scores by from) {
    sorted = order tf_idf_scores by score desc;
    top_30 = limit sorted 20;
    generate group as from, top_30.(word, score) as top_30;
};

-- store in avro format, and publish to mongodb
STORE top_30 INTO '/tmp/ngrams.avro' USING AvroStorage();
STORE top_30 INTO 'mongodb://localhost/agile_data.top_terms_per_sender' USING MongoStorage();

