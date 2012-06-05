register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

sent_distributions = load '/tmp/sent_distributions.avro' using AvroStorage();
store sent_distributions into 
  'mongodb://$user:$password@sawyer.mongohq.com:10001/agile_data.sent_dist' using MongoStorage();
/*store sent_distributions into 'mongodb://fake:user@localhost/agile_data.sent_dist_two' using MongoStorage();
*/