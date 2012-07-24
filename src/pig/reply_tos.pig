register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

rmf /tmp/sent_mails
rmf /tmp/replies
rmf /tmp/with_reply

/* Get rid of emails with reply_to, as they confuse everything in mailing lists. */
avro_emails = load '/me/tmp/thu_emails' using AvroStorage();
clean_emails = filter avro_emails by (froms is not null) and (reply_tos is null);

/* Treat emails without in_reply_to as sent emails */
combined_emails = foreach clean_emails generate froms, tos, message_id;
sent_mails = foreach combined_emails generate flatten(froms.address) as from, 
                                              flatten(tos.address) as to, 
                                              message_id;
-- store sent_mails into '/tmp/sent_mails';
sent_counts = foreach (group sent_mails by (from, to)) generate flatten(group) as (from, to), 
                                                                COUNT_STAR(sent_mails) as total;
-- store sent_counts into '/tmp/sent_counts';

/* Treat in_reply_tos separately, as our FLATTEN() will filter otu the nulls */
avro_emails2 = load '/me/tmp/thu_emails' using AvroStorage();
replies = filter avro_emails2 by (froms is not null) and (reply_tos is null) and (in_reply_to is not null);
replies = foreach replies generate flatten(froms.address) as from,
                                   flatten(tos.address) as to,
                                   in_reply_to;
replies = filter replies by in_reply_to != 'None';
-- store replies into '/tmp/replies';

/* Now join a copy of the emails by message id to the in_reply_to of our emails */
with_reply = join sent_mails by message_id, replies by in_reply_to;
-- store with_reply into '/tmp/with_reply';

/* Filter out mailing lists - only direct replies where from/to match up */
direct_replies = filter with_reply by (sent_mails::from == replies::to) and (sent_mails::to == replies::from);
-- store direct_replies into '/tmp/direct_replies';
direct_replies = foreach direct_replies generate sent_mails::from as from, sent_mails::to as to;
reply_counts = foreach (group direct_replies by (from, to)) generate flatten(group) as (from, to), 
                                                                     COUNT_STAR(direct_replies) as total;
-- store reply_counts into '/tmp/reply_counts';

/* Join sent counts and replied counts to get the reply rates */
sent_replies = join sent_counts by (from, to), reply_counts by (from, to);
reply_ratios = foreach sent_replies generate sent_counts::from as from, 
                                            sent_counts::to as to, 
                                            (float)reply_counts::total/(float)sent_counts::total as ratio;
reply_ratios = foreach reply_ratios generate from, to, (ratio > 1.0 ? 1.0 : ratio) as ratio;
-- store reply_ratios into '/tmp/reply_ratios';

reply_ratios_2 = load '/tmp/reply_ratios' as (from:chararray, to:chararray, ratio:float);
both_sides = join reply_ratios by (from, to), reply_ratios_2 by (to, from);
reciprocation = foreach both_sides generate reply_ratios::from as from,
                                            reply_ratios::to as to,
                                            (float)reply_ratios::ratio/(float)reply_ratios_2::ratio as skew;
store reciprocation into '/tmp/reciprocation';

