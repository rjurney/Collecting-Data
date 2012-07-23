register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

set aggregate.warning 'true';
import 'macros.pig';

rmf /tmp/with_reply
rmf /tmp/sent_totals
rmf /tmp/reply_counts
rmf /tmp/senders_repliers
rmf /tmp/reply_ratio.avro

/* Get all unique combinations of from/reply_to and to/cc/bcc */
avro_emails = load '/me/tmp/thu_emails' using AvroStorage();
avro_emails = filter avro_emails by (froms is not null);
pairs = from_to_pairs(avro_emails);

/* Just replies */
just_replies = filter pairs by in_reply_to is not null;

/* Get the number of total emails sent between addresses */
sent_totals = foreach (group pairs by (from, to)) generate flatten(group) as (from, to), COUNT(pairs) as total;
store sent_totals into '/tmp/sent_totals';

also_emails = load '/me/tmp/thu_emails' using AvroStorage();
also_emails = filter also_emails BY (froms is not null);
also_pairs = from_to_pairs(also_emails);

/* Now join a copy of the emails by message id to the in_reply_to of our emails */
with_reply = join just_replies by in_reply_to, also_pairs by message_id;
store with_reply into '/tmp/with_reply';

replies = foreach with_reply generate just_replies::from as replier, also_pairs::from as sender;
reply_counts = foreach (group replies by (sender, replier)) generate FLATTEN(group) as (sender, replier), COUNT(replies) as total;
store reply_counts into '/tmp/reply_counts';

/* Now join sent totals and reply counts */
senders_repliers = join sent_totals by (from, to), reply_counts by (sender, replier);
store senders_repliers into '/tmp/senders_repliers';

/* Now convert to sent/replies */
reply_ratio = foreach senders_repliers generate from as from, 
                                                to as to, 
                                                sent_totals::total as total_sent,
                                                reply_counts::total as total_replies,
                                                (float)reply_counts::total/(float)sent_totals::total as reply_ratio:float;
store reply_ratio into '/tmp/reply_ratio.avro' using AvroStorage();
