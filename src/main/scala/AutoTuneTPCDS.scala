import org.apache.spark.sql.SparkSession
import org.autotune.exampleConfigs.SparkTuneableConf
import org.autotune._
import org.scalatest.FunSuite
import com.databricks.spark.sql.perf.tpcds.TPCDS

class AutoTuneTPCDS extends FunSuite {

  test("pop is invoked on an empty stack") {
    val tuner = new AutoTuneDefault[SparkTuneableConf](new SparkTuneableConf)

    val cfg = tuner.start.getConfig
    val spark = cfg
      .setConfig(SparkSession.builder.appName("Java Spark SQL data sources example").master("local[*]"))
      .config("spark.sql.perf.results", "/Users/KevinRoj/Desktop/testdata")
      .getOrCreate

    val sqlContext = spark.sqlContext

    import com.databricks.spark.sql.perf.tpcds.Tables
    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    // scaleFactor - Volume of data to generate in GB
    val tables = new Tables(sqlContext, "/Users/KevinRoj/Desktop/tpcds-kit-master/tools", 1)
    // Generate data.
    //tables.genData("/Volumes/Externe SSD/BigData", "parquet", true, true, true, true, false)
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    //tables.createExternalTables("/Volumes/Externe SSD/BigData", format, databaseName, overwrite)
    // Or, if you want to create temporary tables
    tables.createTemporaryTables("/Volumes/Externe SSD/BigData", "parquet")
    // Setup TPC-DS experiment

    val tpcds = new TPCDS(sqlContext = sqlContext)

    val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
    experiment.waitForFinish(60*60*10)

    val resultTable = spark.read.json(experiment.resultPath).select("iteration", "results").show()
  }
}