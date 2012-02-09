require 'rubygems'
require 'sinatra'
require 'sinatra/mongo'
require 'json'
require 'erb'
require 'pp'

set :mongo, 'mongo://localhost/agile_data'
set :erb, :trim => '-'

helpers do
  def to_key(hour)
    if hour < 10
      "0" + hour.to_s
    else
      hour.to_s
    end
  end
end

get '/' do
  erb :index
end

get '/sent_counts/:from/:to' do |from, to|
  @data = mongo['sent_counts'].find_one({:from => from, :to => to})
  erb :'partials/table'
end

get '/to_from_subject' do
  @data = mongo['to_from_subject'].find()
  erb :'partials/read_write_emails'
end

get '/sent_distributions/:email' do |@email|
  raw_data = mongo['sentdist'].find_one({:email => @email})['sent_dist']
  puts JSON raw_data
  @data = (0..23).map do |hour|
    key = to_key(hour)
    value = Integer  
    index = raw_data.find_index{ |record| record['sent_hour'] == key }
    if index
      value = raw_data[index]['total']
    else
      value = 0
    end
    {'sent_hour' => key, 'total' => value}
  end

  @json = JSON @data
  erb :'partials/distribution'
end

get '/top_friends/:email' do |@email|
  @data = mongo['top_friends'].find_one({:email => @email})['top_20']
  erb :'partials/cloud'
end