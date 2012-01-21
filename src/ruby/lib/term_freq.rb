#!/usr/bin/env ruby

require 'rubygems'

word_counts = {}
word_counts.default = 0
STDIN.read.split("\n").each do |line|
   line.strip!
   line = line.downcase.gsub(/[^a-z ]/, '')
   words = line.split /\s+/
   words.each do |word|
     word_counts[word] += 1
   end
end

chart_data = word_counts.sort_by {|k,v| v}
chart_data.each do |key, value|
  puts "#{key}\t#{value}"
end