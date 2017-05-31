import org.scalatest.FunSuite

class AutoTuneTPCDSTest extends FunSuite{

  test("autotune benchmark") {

    {
      AutoTuneTPCDS.main(Array("interactiveQueries"))
    }

  }
}