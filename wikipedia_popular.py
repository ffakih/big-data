#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 19 18:48:09 2021

@author: firasfakih
"""

import sys
from pyspark.sql import SparkSession, functions, types
import re

spark = SparkSession.builder.appName('wikipedia popular').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('times_requested', types.LongType()),
    types.StructField('bytes', types.LongType()),
 
])

def get_filename(path):
    #https://stackoverflow.com/questions/46284824/renaming-the-filename-with-regex-in-python-using-re
    fname = re.search("[0-9]{8}\-[0-9]{2}",path)
    return fname.group(0)
    

def main(in_directory, out_directory):
    pages = spark.read.csv(in_directory, sep = " ",schema = wiki_schema).withColumn('filename',functions.input_file_name())
    
    path_to_hour = functions.udf(lambda path: get_filename(path),returnType=types.StringType())
    
    # Cleaning data per requirements
    data = pages.filter(pages['language'] == 'en')
    data = data.filter(data['title'] != 'Main_page')
    #https://sparkbyexamples.com/spark/spark-filter-startswith-endswith-examples/
    data = data.filter(~data['title'].startswith('Special:'))
    
    # create date column to find hours
    data = data.withColumn('hour',path_to_hour(data.filename))
    
    grouped = data.groupBy('hour')
    max_data = grouped.agg(functions.max(data['times_requested']).alias('times_requested'))
    max_data = max_data.cache()
    out = max_data.join(data,on = ["times_requested","hour"])
    out = out.drop('language','bytes','filename')


    # Used reddit template
    out.write.csv(out_directory + '-wikipedia', mode='overwrite')

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)