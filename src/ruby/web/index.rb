require 'rubygems'
require 'sinatra'
require 'sinatra/mongo'
require 'json'
require 'erb'

set :mongo, 'mongo://localhost/agile_data'

get '/' do
  erb :index
end

get '/sent_counts/:from/:to' do |from, to|
  @data = mongo['sent_counts'].find_one({:from => from, :to => to})
  erb :table
end