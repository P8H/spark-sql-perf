import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.autotune._
import org.autotune.exampleConfigs.SparkTuneableConf

object AutoTuneTPCDS{
  val pathDataset = "hdfs://141.100.62.105:54310/user/istkerojc/tpcds3/bigdata30"

  private def getSparkBuilder(): SparkSession.Builder ={
    return SparkSession.builder.
      appName("TPCDS AutoTune Benchmark")
      //.master("local[*]") //-Dspark.master="local[*]"
      //.config("spark.sql.perf.results", "/tmp/results"); //directory for results
      // better: -Dspark.sql.perf.results="/tmp/results"
  }

  private def reduceLogLevel(spark: SparkSession) = {
    val sparkContextD = JavaSparkContext.fromSparkContext(spark.sparkContext)
    sparkContextD.setLogLevel("ERROR")
  }

  private def runBenchmark(spark: SparkSession, benchmarkTypeStr: String) : Double = {
    val sqlContext = spark.sqlContext

    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    // scaleFactor - Volume of data to generate in GB
    val tables = new Tables(sqlContext, "/Users/KevinRoj/Desktop/tpcds-kit-master/tools", 30)

    tables.createTemporaryTables(pathDataset, "parquet")

    val tpcds = new TPCDS(sqlContext = sqlContext)
    var benchmark = tpcds.tpcds2_4Queries //default value
    benchmarkTypeStr match {
      case "tpcds2_4Queries" => benchmark = tpcds.tpcds2_4Queries
      case "tpcds1_4Queries" => benchmark = tpcds.tpcds1_4Queries
      case "impalaKitQueries" => benchmark = tpcds.impalaKitQueries
      case "interactiveQueries" => benchmark = tpcds.interactiveQueries
      case "deepAnalyticQueries" => benchmark = tpcds.deepAnalyticQueries
    }
    val experiment = tpcds.runExperiment(benchmark)
    experiment.waitForFinish(60*60*10)

    //get result
    val runtime = spark.read.json(experiment.resultPath)
      .select(col("iteration"), org.apache.spark.sql.functions.explode(col("results")))
      .withColumn("TOTAL", expr("col.analysisTime + col.executionTime + col.optimizationTime + col.parsingTime + col.planningTime"))
      .select(sum("TOTAL")).first().asInstanceOf[GenericRowWithSchema].getDouble(0)

    return runtime
  }

  def main(args: Array[String]) {
    val benchmarkTypeStr = args(0)

    val tuner = new AutoTuneDefault[SparkTuneableConf](new SparkTuneableConf)

    { //warm up with default configuration
      val cfg = new SparkTuneableConf
      val spark = cfg.setConfig(getSparkBuilder()).getOrCreate
      val sqlContext = spark.sqlContext

      val tables = new Tables(sqlContext, "/Users/KevinRoj/Desktop/tpcds-kit-master/tools", 30)

      tables.createTemporaryTables(pathDataset, "parquet")
      val tpcds = new TPCDS(sqlContext = sqlContext)

      var benchmark = tpcds.tpcds2_4Queries //default value
      benchmarkTypeStr match {
        case "tpcds2_4Queries" => benchmark = tpcds.tpcds2_4Queries
        case "tpcds1_4Queries" => benchmark = tpcds.tpcds1_4Queries
        case "impalaKitQueries" => benchmark = tpcds.impalaKitQueries
        case "interactiveQueries" => benchmark = tpcds.interactiveQueries
        case "deepAnalyticQueries" => benchmark = tpcds.deepAnalyticQueries
      }
      val experiment = tpcds.runExperiment(benchmark)
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

      val cost = runBenchmark(spark, benchmarkTypeStr)
      spark.stop()

      tuner.addCost(cost)
      tuner.end()

      {
        t += 1; t - 1
      }
    }


  }
}