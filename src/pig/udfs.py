#!/usr/bin/python

import time

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

