register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar
register /me/mongo-hadoop/mongo-2.7.3.jar
register /me/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0.jar
register /me/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
define substr org.apache.pig.piggybank.evaluation.string.SUBSTRING();
define tohour org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

define extract_time(relation, field_in, field_out) RETURNS times {
  $times = foreach $relation generate flatten($field_in.(address)) as $field_out,
                             substr(tohour(date), 11, 13) as sent_hour;
};

register 'udfs.py' using jython as funcs;

rmf /tmp/sent_distributions.avro
rmf /tmp/filled_distributions.avro
-- sh mongo agile_data --eval 'db.sent_dist.drop\(\)'

emails = load '/me/tmp/thu_emails/' using AvroStorage();
filtered = filter emails BY (froms is not null) and (date is not null);

/* Some emails that users send to have no from entries, list email lists.  These addresses
   have reply_to's associated with them.  Here we split reply_to processing off to ensure
   reply_to addresses get credit for sending emails. */
split filtered into has_reply_to if (reply_tos is not null), just_froms if (reply_tos is null);

/* For emails with a reply_to, count both the from and the reply_to as a sender. */
reply_to = extract_time(has_reply_to, reply_tos, address);
reply_to_froms = extract_time(has_reply_to, froms, address);
my_froms = extract_time(just_froms, froms, address);
all_froms = union reply_to, reply_to_froms, my_froms;

pairs = foreach all_froms generate address as email, 
                                   sent_hour;

sent_times = foreach (group pairs by (email, sent_hour)) generate flatten(group) as (email, sent_hour), 
                                                                  (int)COUNT(pairs) as total;

/* Note the use of a sort inside a foreach block */
sent_distributions = foreach (group sent_times by email) { 
    solid = filter sent_times by (sent_hour is not null) and (total is not null);
    sorted = ORDER solid by sent_hour;
    generate group as email, sorted.(sent_hour, total) as sent_dist;
};
store sent_distributions into '/tmp/sent_distributions.avro' using AvroStorage();
store sent_distributions into 'mongodb://localhost/agile_data.sent_dist' using MongoStorage();

-- sent_distributions = load '/tmp/sent_distributions.avro' using AvroStorage();
-- filled_dist = foreach sent_distributions generate email, funcs.fill_in_blanks(sent_dist) as sent_dist;
                                                        
-- store filled_dist into '/tmp/filled_distributions.avro' using AvroStorage();
-- store filled_dist into 'mongodb://localhost/agile_data.sent_dist' using MongoStorage();
-- store sent_distributions into 'mongodb://$user:$password@polarbear.member1.mongohq.com:10004/agile_data.sent_dist' using MongoStorage();