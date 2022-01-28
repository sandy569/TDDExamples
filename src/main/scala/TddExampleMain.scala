//import TddExampleMain.{df, sqlContext}
import TddExample.spark
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit, typedLit, udf}

object TddExampleMain {
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("SparkByExamples.com")
    .getOrCreate()



  def main(args: Array[String]): Unit = {





    spark.sparkContext.setLogLevel("ERROR")

    val sqlContext: SQLContext = spark.sqlContext
    val filePath = "src/main/resources/zipcodes.csv"
    val df = readCSVFile(spark, sqlContext,filePath)
    //read csv with options
    val states: Map[String, String] = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries: Map[String, String] = Map(("US","United States of America"),("IN","India"))

    val addStateDf = transformDf(spark,sqlContext,df,states,countries)
    df.show()


  }
  def transformDf(spark: SparkSession, sqlContext: SQLContext, df: DataFrame,states:Map[String, String] ,countries:Map[String, String] ) :DataFrame = {
  //  val testMapCol = typedLit(countries)
    //print(testMapCol("IN"))
   //val df2: DataFrame = df.withColumn("State_Full_Name",coalesce(testMapCol(df("country")), lit("Not Found")))
   // val df2: DataFrame = df.withColumn("State_Full_Name", getMapValue(countries, "")(df("country")))
    val stateValue = udf(getStateValue)
    val df2 = df.withColumn("State_Full_Name",stateValue(df("country")))
     df2.show()
    df2
  }


  def readCSVFile(spark: SparkSession, sqlContext: SQLContext,filePath:String): DataFrame = {
    val df: DataFrame = spark.read.option("inferSchema", "true").option( "delimiter",",").option( "header" , "true")
      .csv(filePath)
    // df.show()
    df
  }
  import org.apache.spark.sql.functions._
  import spark.implicits._
  def getMapValue(m: Map[String, String], defaultValue: String) = udf{
    (country: String) => m.get(country)
  }
  val m: Map[String, String] = Map(("US","United States of America"),("IN","India"))
  val  getStateValue = (colValue: String) =>{
     m.get(colValue)

  }
}