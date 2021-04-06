ASSIGN_NAME = 'CC_assign3'

# Master's IP address
MASTER_IP = '10.56.0.92'
HDFS_PORT = '54310'
HDFS_INPUT = 'hw2-input'

# What are the top 3 crime types (use OFNS_DESC) that were reported in the month of July (use RPT_DT)?
TOP_N = 3
OFNS_DESC = 7
RPT_DT = 5
JULY = '07'
# PETIT LARCENY
# HARRASSMENT
# ASSAULT 3 & RELATED OFFENSES

# Crime types should be ranked based on the number of crimes reported in the month of July.

# How many crimes of type DANGEROUS WEAPONS were reported in the month of July ?
DANGEROUS_WEAPONS = 'DANGEROUS WEAPONS'
# 701

from csv import reader
from pyspark import SparkContext
# import numpy as np
import operator

sc = SparkContext(appName=ASSIGN_NAME)
sc.setLogLevel("ERROR")

# print("hdfs://{}:54310/hw2-input/".format(MASTER))
data = sc.textFile("hdfs://{}:{}/{}/".format(MASTER_IP, HDFS_PORT, HDFS_INPUT))

# use csv reader to split each line of file into a list of elements.
# this will automatically split the csv data correctly.
splitdata = data.mapPartitions(lambda x: reader(x))

# Filter out most of the data that does not relate to July and is a crime
splitdata = splitdata.filter(lambda x: x[RPT_DT].startswith(JULY) and x[OFNS_DESC])

# Every crime reported in July
splitdata = splitdata.map(lambda x: x[OFNS_DESC].strip())

splitdata = splitdata.countByValue()

print('TOP {} CRIME TYPES ON JULY:'.format(TOP_N))
for crime in sorted(splitdata.items(), key=operator.itemgetter(1),reverse=True)[:TOP_N]:
    print(crime)

print
print DANGEROUS_WEAPONS, splitdata[DANGEROUS_WEAPONS]