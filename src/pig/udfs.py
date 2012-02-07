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

def hour(iso_string):
  tuple_time = time.strptime(iso_string, "%Y-%m-%dT%H:%M:%S")
  return str(tuple_time[2])

