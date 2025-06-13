import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def main():

    if len(sys.argv) != 3:
        print('Usage job.py <входная_директория> <выходная_директория>')
        sys.exit(1)

    print('Starting job.py')

    in_dir = sys.argv[1]
    out_dir = sys.argv[2]

    conf = SparkConf().setAppName('Example - Python')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    df = sql.read.parquet(in_dir)
    print('DataFrame loaded')

    age_stat = df.groupBy('age').count()
    print('DataFrame grouped by age')

    job_id = dict(sc._conf.getAll())['spark.yarn.tags'].replace('dataproc_job_', '')
    print(f'Job ID: {job_id}')
    
    print('Writing output to', out_dir)
    if out_dir.startswith('s3a://'):
        age_stat.repartition(1).write.format('csv').save(out_dir + job_id)
        print('Output written to S3')
    else:
        default_fs = sc._jsc.hadoopConfiguration().get('fs.defaultFS')
        age_stat.repartition(1).write.format('csv').save(default_fs + out_dir + job_id)
        print('Output written to HDFS')

    print('Job completed successfully')

if __name__ == '__main__':
    main()
