register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/mongo-hadoop/mongo-2.3.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0-SNAPSHOT.jar
register /me/mongo-hadoop/pig/target/mongo-pig-1.0-SNAPSHOT.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
define substr org.apache.pig.piggybank.evaluation.string.SUBSTRING();
define tohour org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

rmf /tmp/sent_distributions.avro

/* Get email address pairs for each type of connection, and union them together */
emails = load '/me/tmp/test_inbox' using AvroStorage();

filtered = filter emails BY (from is not null) and (date is not null);
flat = foreach filtered generate flatten(from) as from, 
                                 substr(tohour(date), 11, 13) as sent_hour;  
               
pairs = foreach flat generate LOWER(from) as email, 
                              sent_hour;

sent_times = foreach (group pairs by (email, sent_hour)) generate flatten(group) as (email, sent_hour), 
                                                                  (chararray)COUNT(pairs) as total;

/* Note the use of a sort inside a foreach block */
sent_distributions = foreach (group sent_times by email) { 
                                                          solid = filter sent_times by (sent_hour is not null) and (total is not null);
                                                          sorted = ORDER solid by sent_hour;
                                                          generate group as email, sorted.(sent_hour, total) as sent_dist:bag {column:tuple (sent_hour:chararray, total:chararray)}; 
                                                        };

store sent_distributions into '/tmp/sent_distributions.avro' USING AvroStorage();
store sent_distributions into 'mongodb://localhost/agile_data.sentdist' using MongoStorage();