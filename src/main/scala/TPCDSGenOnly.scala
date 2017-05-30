import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.spark.sql.SparkSession


object TPCDSGenOnly {

  def main (arg: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TPCDS Data Generation").getOrCreate()

    val sqlContext = spark.sqlContext

    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    // scaleFactor - Volume of data to generate in GB (?)
    val tables = new Tables(sqlContext, ".", 2)
    // Generate data.
    tables.genData("hdfs://141.100.62.105:54310/user/istkerojc/tpcds2", "parquet", true, true, true, true, false)

  }
}