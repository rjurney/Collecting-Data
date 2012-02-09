require 'rubygems'
require 'sinatra'
require 'sinatra/mongo'
require 'json'
require 'erb'
require 'pp'

set :mongo, 'mongo://localhost/agile_data'
set :erb, :trim => '-'

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
  @data = (0..23).map do |hour|
    key = String.new
    if hour < 10
      key = "0" + hour.to_s
    else
      key = hour.to_s
    end
    
    value = Integer  
    if raw_data[hour] and raw_data[hour]['total']
      value = raw_data[hour]['total']
    else
      value = 0
    end
    {'sent_hour' => key, 'total' => value}
  end

  @json = JSON @data
  puts @json
  erb :'partials/distribution'
end
