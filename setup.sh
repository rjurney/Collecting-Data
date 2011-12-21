echo "Installing Pig from trunk..."
mkdir lib
cd lib
git clone https://github.com/apache/pig.git
cd pig

# Patch PIG-2411 so we can use the AvroStorage UDF - see https://issues.apache.org/jira/browse/PIG-2411
cp ../../src/avro/avrobug.patch .
patch -p0 < avrobug.patch

ant
cd contrib/piggybank/java
ant
cd ../../../..

echo "Setting up pig environment..."
export CLASSPATH=$CLASSPATH:$PATH/lib/pig/build/ivy/lib/Pig/avro-1.4.1.jar\
:$PATH/lib/pig/build/ivy/lib/Pig/json-simple-1.1.jar\
:$PATH/lib/pig/contrib/piggybank/java/piggybank.jar\
:$PATH/lib/pig/build/ivy/lib/Pig/jackson-core-asl-1.6.0.jar\
:$PATH/lib/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.6.0.jar

cd ..

echo "Setup done!"
