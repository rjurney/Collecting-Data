REGISTER /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
REGISTER /me/pig/contrib/piggybank/java/piggybank.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
REGISTER /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
rmf '/tmp/mail_pairs.avro'

messages = LOAD '/tmp/10000_emails.avro' USING AvroStorage();
smaller = FOREACH messages GENERATE from, to;
pairs = FOREACH smaller GENERATE from, FLATTEN(to) AS to:chararray;

STORE pairs INTO '/tmp/mail_pairs.avro' USING AvroStorage();
