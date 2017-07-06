import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.autotune._
import org.autotune.config.SparkTuneableConfig


object AutoTuneTPCDS{
  val pathDataset = "hdfs://141.100.62.105:54310/user/istkerojc/tpcds2/BigData3"

  private def getSparkBuilder(number: Number = 0, defaultValues: Boolean = false): SparkSession.Builder = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (defaultValues) {
      return SparkSession.builder.
        appName("TPCDS AutoTune Benchmark DefaultValues")
        .config(new SparkConf(true))
        .enableHiveSupport()
        .config("spark.sql.perf.results", "hdfs://141.100.62.105:54310/tmp/tpcds")
        .config("spark.sql.crossJoin.enabled", true) //workaround for some queries
    } else {
      return SparkSession.builder.
        appName("TPCDS AutoTune Benchmark" + number)
        .enableHiveSupport()
        .config("spark.sql.perf.results", "hdfs://141.100.62.105:54310/tmp/tpcds")
        .config("spark.sql.crossJoin.enabled", true) //workaround for some queries
    }

      //.master("local[*]") //-Dspark.master="local[*]"
      //.config("spark.sql.perf.results", "/tmp/results"); //directory for results
      // better: -Dspark.sql.perf.results="/tmp/results"
  }

  private def reduceLogLevel(spark: SparkSession) = {
    val sparkContextD = JavaSparkContext.fromSparkContext(spark.sparkContext)
    sparkContextD.setLogLevel("ERROR")
  }

  private def runBenchmark(spark: SparkSession, benchmarkTypeStr2: Any): Double = {
    val sqlContext = spark.sqlContext

    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    // scaleFactor - Volume of data to generate in GB
    val tables = new Tables(sqlContext, "/Users/KevinRoj/Desktop/tpcds-kit-master/tools", 30)

    tables.createTemporaryTables(pathDataset, "parquet")

    val tpcds = new TPCDS(sqlContext = sqlContext)

    var benchmark = tpcds.tpcds2_4Queries //default value

    benchmarkTypeStr2 match {
      case "tpcds2_4Queries" => benchmark = tpcds.tpcds2_4Queries
      case "tpcds1_4Queries" => benchmark = tpcds.tpcds1_4Queries
      case "impalaKitQueries" => benchmark = tpcds.impalaKitQueries
      case "interactiveQueries" => benchmark = tpcds.interactiveQueries
      case "deepAnalyticQueries" => benchmark = tpcds.deepAnalyticQueries
      case n: Int => benchmark = Seq(tpcds.tpcds2_4Queries.apply(n)) //assume that we want to benchmark a single query
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
    var singleEvaluation = false;

    //warm up with default configuration
    {
      val sparkWarmUp = getSparkBuilder(-1).getOrCreate
      val sqlContext = sparkWarmUp.sqlContext

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
        case _ => singleEvaluation = true
      }
      val experiment = tpcds.runExperiment(benchmark)
      experiment.waitForFinish(60 * 60 * 10)
      sqlContext.clearCache()
      sparkWarmUp.stop()
    }


    /** optional: activate single query evaluation **/
    val evaluationList: Seq[Any] = if (singleEvaluation) Seq.range(0, queryNames.size - 1) else Seq(benchmarkTypeStr)
    for (bench <- evaluationList) {
      val tuner = new AutoTuneDefault[SparkTuneableConfig](new SparkTuneableConfig)

      /** tune section - search for the best configuration **/
      var t: Int = 0
      while ( {
        t < 40
      }) { //40 benchmark tests
        val cfg: SparkTuneableConfig = tuner.start.getConfig
        val spark: SparkSession = cfg.setConfig(getSparkBuilder(t)).getOrCreate
        reduceLogLevel(spark)

        val cost = runBenchmark(spark, bench)
        spark.sqlContext.clearCache()
        spark.stop()

        tuner.addCost(cost)
        tuner.end()

        {
          t += 1
          t - 1
        }
      }

      val cfg: SparkTuneableConfig = new SparkTuneableConfig
      val spark: SparkSession = cfg.setConfig(getSparkBuilder(-2)).getOrCreate
      val cost = runBenchmark(spark, bench)
      println("Cost from last default execution: " + cost)
      println("Cost from best evaluation: " + tuner.getBestResult)
    }


  }

  //tpcds2_4Queries single queries
  val queryNames = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19",
    "q20", "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27",
    "q28", "q29", "q30", "q31", "q32", "q33", "q34", "q35", "q36", "q37",
    "q38", "q39a", "q39b", "q40", "q41", "q42", "q43", "q44", "q45", "q46", "q47",
    "q48", "q49", "q50", "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58",
    "q59", "q60", "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69",
    "q70", "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79",
    "q80", "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89",
    "q90", "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99",
    "ss_max"
  )
}