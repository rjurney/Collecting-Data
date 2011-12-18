require 'rubygems'
require 'avro'
require 'json'

# This is our Avro schema
schema_string = IO.read '../avro/email.schema'

puts "Writing Avros to /tmp/messages.avro..."
file = File.open('/tmp/messages.avro', 'wb')
schema = Avro::Schema.parse(schema_string)
writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)
dw << {"from" => "russell.jurney@gmail.com", "to" => ["test@foo.com"]}
puts "Closing /tmp/messages.avro"
dw.close
