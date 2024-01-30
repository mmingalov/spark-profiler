import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import col, when, count, countDistinct, to_date

def get_stats(tablename,columns_to_exclude, df):
    DF_ROWS_CNT = df.count()

    # getting NOT NULL values count for columns in dataframe
    df_counts = df \
        .select([count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns]) \
        .withColumn('summary', f.lit('counts'))

    # getting UNIQUE values count for columns in dataframe
    df_uniques = df \
        .select([countDistinct(col(c)).alias(c) for c in df.columns]) \
        .withColumn('summary', f.lit('uniques'))

    # getting MISSING values count for columns in dataframe
    df_missing = df_counts \
        .select([(DF_ROWS_CNT - col(c)).alias(c) for c in df_counts.columns[:len(df_counts.columns) - 1]]) \
        .withColumn('summary', f.lit('missing'))

    # getting PCT of MISSING values for columns in dataframe
    df_missing_perc = df_missing \
        .select([(100 * col(c) / DF_ROWS_CNT).cast('int').alias(c) for c in
                 df_missing.columns[:len(df_missing.columns) - 1]]) \
        .withColumn('summary', f.lit('missing_perc'))

    # union of all previous df into common Spark DF
    udf1 = df_counts \
        .union(df_uniques) \
        .union(df_missing) \
        .union(df_missing_perc)

    #---------2--MIN,MAX,AVG for NUMERIC columns
    # we choose all numeric columns except those we mark in exclude list
    columns_to_process = [field for (field, dataType) in df.dtypes
                          if dataType != "string"
                          and dataType != "timestamp"
                          and dataType != "date"
                          and field not in columns_to_exclude]

    # getting MIN values for chosen columns
    df_min = df \
        .select(
        [f.min(col(c)).cast('decimal(8,2)').alias(c) if c in columns_to_process else f.lit(None).alias(c) for c in
         df.columns]) \
        .withColumn('summary',f.lit('min'))

    # getting MAX values for chosen columns
    df_max = df \
        .select(
        [f.max(col(c)).cast('decimal(8,2)').alias(c) if c in columns_to_process else f.lit(None).alias(c) for c in
         df.columns]) \
        .withColumn('summary', f.lit('max'))

    # getting AVG values for chosen columns
    df_avg = df \
        .select(
        [f.avg(col(c)).cast('decimal(8,2)').alias(c) if c in columns_to_process else f.lit(None).alias(c) for c in
         df.columns]) \
        .withColumn('summary', f.lit('avg'))

    udf2 = df_min \
        .union(df_max) \
        .union(df_avg)

    #-------------3--MIN,MAX for DATE columns
    # we choose all date columns except those we mark in exclude list
    columns_to_process = [field for (field, dataType) in df.dtypes
                          if (dataType == 'timestamp' or dataType == 'date')
                          and field not in columns_to_exclude]

    # getting MIN values for chosen DATE columns
    df_min_dt = df \
        .select(
        [f.min(to_date(col(c),"yyyy-MM-dd")).alias(c) if c in columns_to_process else f.lit(None).alias(c) for c in
         df.columns]) \
        .withColumn('summary',f.lit('min_dt'))

    # getting MAX values for chosen DATE columns
    df_max_dt = df \
        .select(
        [f.max(to_date(col(c), "yyyy-MM-dd")).alias(c) if c in columns_to_process else f.lit(None).alias(c) for c in
         df.columns]) \
        .withColumn('summary', f.lit('max_dt'))

    udf3 = df_min_dt \
        .union(df_max_dt)

    return udf1, udf2, udf3



tablename = 'SourceTable'
excludedCols = ['city_nm','product_nm','t_deleted_flg']
df = ... #getting data from hadoop environment
df1, df2, df3 = get_stats(tablename, excludedCols, df) #where df is spark dataframe we red from source

# now we have calculated statistics for our table. It is split by 3 spark DF. We can merge these spark DF to common Pandas DF and save on disk
import pandas

dfp1 = df1.toPandas()
dfp2 = df2.toPandas()
dfp3 = df3.toPandas()

dfp1_transpose = dfp1.transpose()
dfp2_transpose = dfp2.transpose()
dfp3_transpose = dfp3.transpose()

dfp1_transpose.columns = ['counts', 'uniques', 'missing', 'missing_perc']
dfp2_transpose.columns = ['min', 'max', 'avg']
dfp3_transpose.columns = ['min_dt', 'max_dt']

# do merge by Index
dfp12 = pandas.merge(dfp1_transpose, dfp2_transpose, left_index=True, right_index=True)
dfp123 = pandas.merge(dfp12, dfp3_transpose, left_index=True, right_index=True)
dfp123["table_name"] = tablename
