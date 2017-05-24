import org.apache.spark.sql.SparkSession
import org.autotune.exampleConfigs.SparkTuneableConf
import org.autotune._
import org.scalatest.FunSuite
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

class AutoTuneTPCDS extends FunSuite {

  private def getSparkBuilder(): SparkSession.Builder ={
    return SparkSession.builder.
      appName("TPCDS AutoTune Benchmark")
      .master("local[*]")
      .config("spark.sql.perf.results", "/Users/KevinRoj/Desktop/testdata"); //directory for results
  }

  private def reduceLogLevel(spark: SparkSession) = {
    val sparkContextD = JavaSparkContext.fromSparkContext(spark.sparkContext)
    sparkContextD.setLogLevel("ERROR")
  }

  private def runBenchmark(spark: SparkSession) : Double = {
    val sqlContext = spark.sqlContext

    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    // scaleFactor - Volume of data to generate in GB
    val tables = new Tables(sqlContext, "/Users/KevinRoj/Desktop/tpcds-kit-master/tools", 1)
    // Generate data.
    //tables.genData("/Volumes/Externe SSD/BigData", "parquet", true, true, true, true, false)

    tables.createTemporaryTables("/Volumes/Externe SSD/BigData", "parquet")
    val tpcds = new TPCDS(sqlContext = sqlContext)
    val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
    experiment.waitForFinish(60*60*10)

    //get result
    val runtime = spark.read.json(experiment.resultPath)
      .select(col("iteration"), org.apache.spark.sql.functions.explode(col("results")))
      .withColumn("TOTAL", expr("col.analysisTime + col.executionTime + col.optimizationTime + col.parsingTime + col.planningTime"))
      .select(sum("TOTAL")).first().asInstanceOf[GenericRowWithSchema].getDouble(0)

    return runtime
  }

  test("autotune benchmark") {
    val tuner = new AutoTuneDefault[SparkTuneableConf](new SparkTuneableConf)

    {
      val cfg = new SparkTuneableConf //warm up with default configuration //tuner.start.getConfig
      val spark = cfg.setConfig(getSparkBuilder()).getOrCreate
      val sqlContext = spark.sqlContext

      val tables = new Tables(sqlContext, "/Users/KevinRoj/Desktop/tpcds-kit-master/tools", 1)
      // Generate data.
      //tables.genData("/Volumes/Externe SSD/BigData", "parquet", true, true, true, true, false)

      tables.createTemporaryTables("/Volumes/Externe SSD/BigData", "parquet")
      val tpcds = new TPCDS(sqlContext = sqlContext)
      val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
      experiment.waitForFinish(60*60*10)
    }



    /** tune section - search for the best configuration **/

    var t: Int = 0
    while ( {
      t < 40
    }) { //40 benchmark tests
      val cfg: SparkTuneableConf = tuner.start.getConfig
      val spark: SparkSession = cfg.setConfig(getSparkBuilder).getOrCreate
      reduceLogLevel(spark)

      val cost = runBenchmark(spark)
      spark.stop()

      tuner.addCost(cost)
      tuner.end()

      {
        t += 1; t - 1
      }
    }


  }
}