#!/usr/bin/python

import time
import email
import datetime

@outputSchema("isodatetime:chararray")
def parse_date(self, date_string):
  tuple_time = email.utils.parsedate(date_string)
  iso_time = time.strftime("%Y-%m-%dT%H:%M:%S", tuple_time)
  return iso_time

@outputSchema("word:chararray")
def strip(word):  
  return word.lstrip

@outputSchema("t:(word:chararray,num:long)")
def complex(word):  
  return (str(word),long(word)*long(word))

@outputSchemaFunction("squareSchema")
def square(num):
  return ((num)*(num))

@schemaFunction("squareSchema")
def squareSchema(input):
  return input

# No decorator - bytearray
def concat(str):
  return str+str

# Extracts the hour from an iso8601 datetime string
@outputSchema("hour:chararray")
def hour(iso_string):
  tuple_time = time.strptime(iso_string, "%Y-%m-%dT%H:%M:%S")
  return str(tuple_time[2])

# Given from, to email address pairs and a bag of their subjects, return from, to and a bag of word counts
# Input format: {group: (from: chararray,to: chararray),subjects: {(subject: chararray)}}
@outputSchema("t:(from:chararray, to:chararray, word_counts:bag{t2:(word:chararray, total:int)})")
def word_count_subjects(group, subjects):
  to = group[0]
  _from = group[1]
  word_counts = {}
  for subject in subjects:
    words = subject[0].split()
    for word in words:
      word_counts[word] = word_counts.get(word, 0) + 1
  return to, _from, sorted(word_counts.items(), key=lambda word_count: word_count[1], reverse=True)

@outputSchema("bag:{t2:(word:chararray, total:int)}")
def bag_test(group, subjects):
  word_counts = {}
  for subject in subjects:
    words = subject[0].split()
    for word in words:
      word_counts[word] = word_counts.get(word, 0) + 1
  return sorted(word_counts.items(), key=lambda word_count: word_count[1], reverse=True)