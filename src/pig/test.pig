REGISTER ./lib/pig-0.9.1/contrib/piggybank/java/piggybank.jar
REGISTER ../elephant-bird/dist/elephant-bird-2.1.2.jar

DEFINE ThriftLoader com.twitter.elephantbird.pig.load.LzoThriftB64LinePigLoader();

emails = LOAD '/tmp/emails.dat' USING com.twitter.elephantbird.pig.load.LzoThriftB64LinePigLoader('com.datasyndrome.thrift.Email')

-- Remove re-used file locations each time
rmf /tmp/per_user.avro

messages = LOAD '/tmp/emails.avro' USING AvroStorage();

user_groups = GROUP messages by user_id;
per_user = FOREACH user_groups {                
    sorted = ORDER messages BY message_id DESC;     
    GENERATE CONCAT('messages_per_user_id:', (chararray)group) AS user_key, sorted.$0 AS messages;
}

DESCRIBE per_user
-- per_user: {user_key: chararray,messages: {(message_id: int)}}

STORE per_user INTO '/tmp/per_user.avro' USING AvroStorage();
