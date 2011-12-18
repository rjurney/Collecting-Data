REGISTER ./lib/pig-0.9.1/build/ivy/lib/Pig/avro-1.4.1.jar
REGISTER ./lib/pig-0.9.1/build/ivy/lib/Pig/json-simple-1.1.jar
REGISTER ./lib/pig-0.9.1/contrib/piggybank/java/piggybank.jar
REGISTER ./lib/pig-0.9.1/build/ivy/lib/Pig/jackson-core-asl-1.6.0.jar
REGISTER ./lib/pig-0.9.1/build/ivy/lib/Pig/jackson-mapper-asl-1.6.0.jar

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

-- Remove re-used file locations each time
rmf /tmp/per_user.avro

messages = LOAD '/tmp/messages.avro' USING AvroStorage();

user_groups = GROUP messages by user_id;
per_user = FOREACH user_groups {                
    sorted = ORDER messages BY message_id DESC;     
    GENERATE CONCAT('messages_per_user_id:', (chararray)group) AS user_key, sorted.$0 AS messages;
}

DESCRIBE per_user
-- per_user: {user_key: chararray,messages: {(message_id: int)}}

STORE per_user INTO '/tmp/per_user.avro' USING AvroStorage();
