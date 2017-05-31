import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.autotune._
import org.autotune.exampleConfigs.SparkTuneableConf
import org.scalatest.FunSuite

class AutoTuneTPCDSTest extends FunSuite{

  test("autotune benchmark") {

    {
      AutoTuneTPCDS.main(Array("interactiveQueries"))
    }

  }
}