import sys
from pyspark.sql import SparkSession, functions, types, Row
import re
import numpy as np

spark = SparkSession.builder.appName('correlate logs').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred. Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        return Row(hostname = m.group(1), bytes_transferred = m.group(2))
    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() objects
    rdd = log_lines.map(line_to_row)
    rdd = rdd.filter(not_none)
    return rdd


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory))

    # TODO: calculate r.
    step2 = logs.groupBy(logs['hostname'])
    
    step2 = step2.agg(functions.count(logs["hostname"]).alias('x_i'),functions.sum(logs['bytes_transferred']).alias('y_i'))
    step3 = step2.groupBy()
    
    n = step2.count()
    # https://stackoverflow.com/questions/37495039/difference-between-spark-rdds-take1-and-first
    x_i = step3.agg(functions.sum(step2['x_i'])).first()[0]
    x_i2 = step3.agg(functions.sum(step2['x_i'] * step2['x_i'])).first()[0]
    y_i = step3.agg(functions.sum(step2['y_i'])).first()[0]
    y_i2 = step3.agg(functions.sum(step2['y_i'] * step2['y_i'])).first()[0]
    xy = step3.agg(functions.sum(step2['x_i'] * step2['y_i'])).first()[0]
    
    num = (n*xy) - (x_i*y_i)
    denom = np.sqrt((n*x_i2) - (x_i**2)) * np.sqrt((n*y_i2) - (y_i**2))
    r = num/denom  # TODO: it isn't zero.
    print("r = %g\nr^2 = %g" % (r, r**2))


if __name__=='__main__':
    in_directory = sys.argv[1]
    main(in_directory)
