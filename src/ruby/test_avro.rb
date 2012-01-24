require 'rubygems'
require 'avro'
require 'pp'

# Test writing avros
SCHEMA = <<-JSON
{ "type": "record",
  "name": "Message",
  "fields" : [
    {"name": "message_id", "type": "int"},
    {"name": "topic", "type": "string"},
    {"name": "user_id", "type": "int"}
  ]}
JSON

file = File.open('/tmp/messages.avro', 'wb')
schema = Avro::Schema.parse(SCHEMA)
writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)
dw << {"message_id" => 11, "topic" => "Hello galaxy", "user_id" => 1}
dw << {"message_id" => 12, "topic" => "Jim is silly!", "user_id" => 1}
dw << {"message_id" => 23, "topic" => "I like apples.", "user_id" => 2}
dw.close

# Test reading avros
file = File.open('/tmp/messages.avro', 'r+')
dr = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)
dr.each do |record|
  pp record
end