/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

/* Avro */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

-- raw_emails = LOAD 's3n://agile.data/emails' using AvroStorage();
raw_emails = LOAD '/enron/enron_messages.tsv' 