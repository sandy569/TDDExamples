
import TddExampleMain.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{Args, BeforeAndAfterAll, Filter, FunSuite, Reporter, Status, Stopper, Suite}
//import org.scalatest.F
import scala.collection.immutable
import scala.runtime.Nothing$
class TddExampleTest extends FunSuite with BeforeAndAfterAll {


  @transient var spark:SparkSession = _
  override def beforeAll(): Unit = {
    spark = SparkSession.builder().
      appName("TDDExapleTeest").master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()

  }
  test("Testcase For Read File"){
    print("sandy")
    val sqlContext: SQLContext = spark.sqlContext
    val filePath = "src/main/resources/zipcodes.csv"
    val df = TddExampleMain.readCSVFile(spark,sqlContext,filePath)
    df.show(5)
    df.select("Country").distinct().show()
    assert(df.count()==21,"rec count should be 9")
  }
  test("Testcase For New Column and Row Count"){
    val sqlContext: SQLContext = spark.sqlContext
    val filePath = "src/main/resources/zipcodes.csv"
    val states: Map[String, String] = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries: Map[String, String] = Map(("US","United States of America"),("IN","India"))
    val df = TddExampleMain.readCSVFile(spark,sqlContext,filePath)
    val df1 = TddExampleMain.transformDf(spark,sqlContext,df,states,countries)
    val cols = df1.columns.contains("State_Full_Name")
    assert(cols == true,"New Column name is present")
    val df1Count = df1.select("State_Full_Name").where(df1("State_Full_Name")==="United States of America").groupBy("State_Full_Name").count().collect()(0)(1)

    //print(df1Count)
    assert(df1Count == 21, "Count is expected")
  }

}

