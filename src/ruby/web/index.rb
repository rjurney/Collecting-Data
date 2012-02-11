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
  
  def mongo_fetch_one(collection, query)
    begin
      data = mongo[collection].find_one(query)
    rescue
      puts "Problem fetching #{query} from #{collection}!"
    end
  end
end

get '/' do redirect '/top_friends/russell.jurney@gmail.com' end

get '/sent_counts/:from/:to' do |from, to|
  @data = mongo_fetch_one('sent_counts', {:from => from, :to => to})
  erb :'partials/table'
end

get '/to_from_subject' do
  @data = mongo['to_from_subject'].find()
  erb :'partials/read_write_emails'
end

get '/sent_distributions/:email' do |@email|
  raw_data = mongo_fetch_one('sentdist', {:email => @email})['sent_dist']

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
  
  # Also include friends
  @friends = mongo_fetch_one('top_friends', {:email => @email})['top_20']
  erb :'partials/distribution'
end

get '/top_friends/:email' do |@email|
  @friends = mongo_fetch_one('top_friends', {:email => @email})['top_20']
  erb :'partials/cloud'
end