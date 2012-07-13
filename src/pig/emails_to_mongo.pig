<<<<<<< HEAD
/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar

/* Avro */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
=======
/* Load Avro jars */
register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

/* Piggybank */
register /me/pig/contrib/piggybank/java/piggybank.jar
>>>>>>> b0e60b6686e631dc34caae5b4ad0e85fcc12f911

/* MongoDB */
register /me/mongo-hadoop/mongo-2.7.3.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0.jar

<<<<<<< HEAD
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

emails = load '/me/tmp/emails' using AvroStorage();
store emails into 'mongodb://localhost/agile_data.emails' using MongoStorage();
=======
/* Set speculative execution for mappers and reducers off, or MongoDB will get duplicate records */
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

/* Define shortform functions for convenience */
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

register 'email_utils.py' using jython as email;

/* Load the emails */
emails = load '/me/tmp/small_inbox' using AvroStorage();

emails = foreach emails generate email.process_email(raw_email, thread_id)

rmf /tmp/test_mail
store email_parts into '/tmp/test_mail';

/* Store the emails into our local MongoDB, 'agile_data' database, 'emails' collection */
/* store emails into 'mongodb://localhost/agile_data.emails' using MongoStorage(); */
>>>>>>> b0e60b6686e631dc34caae5b4ad0e85fcc12f911
