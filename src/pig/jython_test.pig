register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/contrib/piggybank/java/piggybank.jar
register /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
register /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
register 'udfs.py' using jython as myfuncs;

rmf /tmp/jython_test.txt
emails = load '/me/tmp/again_inbox/part-0-0.avro' using AvroStorage();
emails = limit emails 100;
emails = filter emails by (from is not null) and (to is not null);
pairs = foreach emails generate flatten(from) as from, flatten(to) as to, subject;
gft = group pairs by (from, to);
gft = foreach gft generate group, pairs.(subject) as subjects;
to_from_word_counts = foreach gft generate myfuncs.word_count_subjects(group, subjects);

store to_from_word_counts into '/tmp/jython_test.txt';

