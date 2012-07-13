/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

/* Load Avro jars and define shortcut */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

rmf /tmp/foo
enron_messages = LOAD '/me/enron-avro/enron_messages.tsv' AS (

    message_id:chararray,
    sql_date:chararray,
    from_address:chararray,
    from_name:chararray,
    subject:chararray,
    body:chararray

);

define cmd `enron.py` ship('enron.py');

-- raw_emails: {message_id: chararray,date: chararray,from: chararray,to_cc_bcc: chararray,subject: chararray,body: chararray}
address_parts = stream raw_emails through cmd as (message_id:chararray, t:chararray, address:chararray);
store address_parts into '/tmp/foo';
