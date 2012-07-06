/* Get a count of records, return the name of the relation and . */
DEFINE total_count(relation) RETURNS total {
  $total = FOREACH (group $relation all) generate '$relation' as label, COUNT_STAR($relation) as total;
};

/* Get totals on 2 relations, union and return them with labels */
DEFINE compare_totals(r1, r2) RETURNS totals {
  total1 = total_count($r1);
  total2 = total_count($r2);
  $totals = union total1, total2;
};

/* See how many records from a relation are removed by a filter, given a condition */
DEFINE test_filter(original, condition) RETURNS result {
  filtered = filter $original by $condition;
  $result = compare_totals($original, filtered);
};

/* Get from/to/cc/bccs individually from a bag of addresses */
DEFINE get_addresses(in_relation, group_name, field_name) RETURNS addresses {
  between = foreach in_relation generate FLATTEN($group_name) as ($field_name, b);
  $addresses = foreach between generate $field_name as $field_name;
};

/* Given a document relation with a text column and a unique id column, 
   count the number of times each word occurs in each document, and divide that by
   the occurrence of the word among all documents */
/*DEFINE tf_idf(document, text_col, id_col) RETURNS scores_per_document {
  chunks = foreach $document generate id_col, flatten(TOKENIZE(LOWER(text_col))) as word;
};

-- Split the body into words in a bag, and then flatten that bag to create many one-word records
chunks = foreach no_cr generate from, flatten(TOKENIZE(LOWER(body))) as word;

-- Ensure there are alphanumeric characters so that these entries are words.  We can do more here.
words = filter chunks by (word matches '\\w+[^_\\W]');
-- 

-- Trim fields down to just words, since this is a global list and not a per-from list
all_words = foreach words generate from, word;
per_document = distinct all_words;
document_frequency = foreach (group per_document by word) generate group as word, (int)COUNT(per_document) as document_total;

-- Now group words by from and get a word count per email
term_frequency = foreach (group words by (from, word)) generate flatten(group) as (from, word), (int)COUNT(words) as term_total;
*/
/*-- bring the term and document frequencies together 
together = join term_frequency BY word, document_frequency by word;
tf_idf = foreach together generate from as from, 
                                   term_frequency::word as word, 
                                   (double)term_total/(double)document_total as score;

-- For each message_id, store the top 100 scores per from
top_20 = foreach (group tf_idf by from) {
    sorted = order tf_idf by score desc;
    top_20 = limit sorted 20;
    generate group as from, top_20.(word, score) as top_20;
};

-- store in avro format, and publish to mongodb
STORE top_20 INTO '/tmp/ngrams.avro' USING AvroStorage();
STORE top_20 INTO 'mongodb://localhost/agile_data.top_terms_per_sender' USING MongoStorage();*/

