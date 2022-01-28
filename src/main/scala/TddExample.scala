import TddExampleMain.spark
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object TddExample extends App {
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val sqlContext: SQLContext = spark.sqlContext

  val df: DataFrame = spark.read.option("inferSchema", "true").option( "delimiter",",").option( "header" , "true")
    .csv("src/main/resources/zipcodes.csv")

  df.show(5)
}
