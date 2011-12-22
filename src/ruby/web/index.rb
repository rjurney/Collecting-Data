require 'rubygems'
require 'sinatra'
require 'mongo'

@conn = Mongo::Connection.new
@db   = @conn['test']
@coll = @db['pig']

puts "There are #{@coll.count} records. Here they are:"
@coll.find.each { |doc| puts doc.inspect }