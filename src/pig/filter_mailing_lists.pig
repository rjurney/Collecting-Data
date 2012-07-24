register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

set aggregate.warning 'true';
import 'macros.pig';

emails = load '/me/tmp/thu_emails' using AvroStorage();

/* Email lists often reply_to */
has_reply_to = FILTER emails by reply_tos is not null;
short_form = foreach has_reply_to generate froms, tos, reply_tos, message_id, in_reply_to;
r_tos = foreach short_form generate froms, tos, flatten(reply_tos) as (reply_to_name, reply_to_email), message_id, in_reply_to;
just_r = foreach r_tos generate reply_to_email;
reply_to_counts = foreach (group just_r by reply_to_email) generate group as reply_to_email, COUNT_STAR(just_r) as total;

/* Get a count of emails sent from our hero, the inbox owner */
from_to = foreach short_form generate flatten(froms) as (from_name, from_email), flatten(tos) as (to_name, to_email);
just_russ = filter from_to by from_email == 'russell.jurney@gmail.com';
sent_counts = foreach (group just_russ by to_email) generate group as to_email, COUNT_STAR(just_russ) as total;

/* Join reply_to counts and sent_counts to get a ratio for filtering. Our thinking here is that high reply_to counts
   are a signal of mailing list or spam, and sent_counts from our hero are a sign of valid emails. */
both_metrics = join reply_to_counts by reply_to_email, sent_counts by to_email;


sorted = order reply_to_counts by total;
dump sorted