import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import spark.implicits._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalTime}

val spark = SparkSession.builder().
    master("yarn").
    appName("spark").
    config("deployMode","cluster").
    config("hive.exec.dynamic.partition","true").
    config("hive.exec.dynamic.partition.mode","nonstrict").
    config("spark.sql.sources.partitionOverWriteMode","dynamic").
    config("spark.rpc.message.maxSize",2000).
    
    config("spark.executor.cores",24).
    config("spark.executor.instances",24).
    config("spark.sql.shuffle.partitions",576).
    
    config("spark.executor.memory",24g).
    config("spark.driver.memory",8g).
    
    config("spark.dynamicAllocation.enabled","true").
    config("spark.dynamicAllocation.minExecutors",1).
    config("spark.dynamicAllocation.maxExecutors",300).
    config("spark.sql.autoBroadcastJoinThreshold",-1).
    config("spark.sql.adaptive.coalescePartitions.enabled","true").
    enableHiveSupport().
    getOrCreate()
    
def transpose_df(df: DataFrame, t_column_name: String) =  {
    val colsToUnpivot = df.columns diff Array("summary")
    val colToPivot = "summary"

    val structCols = colsToUnpivot.map(cu => struct(lit(cu).as("column_name"),col(cu).as("cu"),col(colToPivot).as("cp")))
    
    df.
    withColumn("flattened",explode(array(structCols: _*))).
    groupBy($"flattened.column_name").pivot($"flattened.cp").agg(first($"flattened.cu")).
    withColumnRenamed("column_name",t_column_name)
}

def nvl(ColIn: Column, ReplaceVal: Any):
    column = {
            (when(ColIn.isNull, lit(ReplaceVal)).otherWise(ColIn))
    }
def selectByType_notEqual(colType: List[DataType], df: DataFrame) = {
    val cols = df.schema.toList.
        filter(x=> !colType.contains(x.datatype)).
        map(c=> col(c.name))
    df.select(cols:_*)
    
}

def selectByType_Equal(colType: List[DataType], df: DataFrame) = {
    val cols = df.schema.toList.
        filter(x=> colType.contains(x.datatype)).
        map(c=> col(c.name))
    df.select(cols:_*)
}
    
val startTime = Instant.now()

val tt = "stats_scala_222"

val TABLE = List(
    "table1"
    ,"table2"
)

val columns_to_exclude = List(
    "t_source_system_id","product_nm","code_version_info","trigger_type"
)

var result_df_list = List[DataFrame]()
for (tablename <- TABLE) {
    val df = spark.table(tablename).persist()
    val DF_ROWS_CNT = df.count()
    
    //getting NOTNULL values count for columns 
    val df_counts = df.
                    select(df.columns.map(c = > count(col(c)).as(s"$c")): _*).
                    withColumn("summary",lit("counts")).
                    persist()
                    
    val df_uniques = df.
                    select(df.columns.map(c => countDistinct(col(c)).as(s@$c)):_*).
                    withColumn("summary",lit("uniques")).
                    persist()
    //getting MISSING values count for columns
    val df_missing = df_counts.
                    select(df_counts.columns.map(c => (col(c)*(-1)+DF_ROWS_CNT).as(s"$c")): _*).
                    withColumn("summary",lit("missing")).
                    persist()
    val df_missing_perc = df_missing.
                        select(df_missing.columns.map(c => (col(c)*100/DF_ROWS_CNT).cast(IntegerType).as(s"$c")): _*).
                        withColumn("summary",lit("missing_perc")).
                        persist()

    val udf1 = df_counts.union(df_uniques).union(df_missing).union(df_missing_perc).persist()
    
    val df_columns_list = (df.columns diff columns_to_exclude).toList
    
    val df2 = selectByType_notEqual(List(StringType,DateType,TimestampType), df.select(df_columns_list.map(m=>col(m)):_*))
    
    val df_min = df.
                select(df2.columns.map(c => min(col(c)).cast("decimal(18,2)").as(s"$c")): _*).
                withColumn("summary",lit("min"))

    val df_max = df.
                select(df2.columns.map(c => max(col(c)).cast("decimal(18,2)").as(s"$c")): _*).
                withColumn("summary",lit("max"))
    val df_avg = df.
                select(df2.columns.map(c => avg(col(c)).cast("decimal(18,2)").as(s"$c")): _*).
                withColumn("summary",lit("avg"))
                
    val udf2 = df_min.
                union(df_max).
                union(df_avg).
                persist()
                
    val df3 = selectByType_Equal(List(DateType,TimestampType), df.select(df_columns_list.map(m=>col(m)):_*))
    
    val df_min_dt = df.
                    select(df3.columns.map(c => min(col(c)).cast(DateType).as(s"$c")): _*).
                    withColumn("summary",lit("min_dt"))
    val df_max_dt = df.
                    select(df3.columns.map(c => max(col(c)).cast(DateType).as(s"$c")): _*).
                    withColumn("summary",lit("max_dt"))
    val udf3 = df_min_dt.
                union(df_max_dt).
                persist()
                
    //transposing DF
    val tudf1 = transpose_df(udf1,"column_name").persist()
    val tudf2 = transpose_df(udf2,"column_name2").persist()
    val tudf3 = transpose_df(udf3,"column_name3").persist()
    
    val df12 = tudf1.join(tudf2, tudf1("column_name") === tudf2("column_name2"),"left")
    
    val df_final = df12.
                    join(tudf3,df12("column_name") === tudf3("column_name3"),"left").
                    drop("column_name2","column_name3").
                    withColumn("table_name",lit(tablename)).
                    withColumn("ind",monotonically_increasing_id()).
                    select("column_name","counts","uniques","missing","missing_perc","min","max","avg","min_dt","max_dt","table_name","ind").
                    persist()
    print("Stats for table " + tablename + " has been calculated")
    df_final.show()
    
    //adding DF to common list
    result_df_list = result_df_list :+ df_final
    
}

val endTime = Instant.now()

val result_df = result_df_list.reduce(_.union(_))
result_df.
        repartition(1).
        write.
        mode("overwrite").
        format("parquet").
        option("header",true).
        csv(tt)
        
spark.catalog.clearCache()



