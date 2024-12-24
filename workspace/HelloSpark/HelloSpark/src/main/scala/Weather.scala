import org.apache.spark.sql.SparkSession
object Weather {


    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Read Partitioned Data")
        .master("local[*]")
        .getOrCreate()


      val pathToData = "C:\\Users\\Aisultan\\Documents\\Spark\\WeatherDS\\weather"

      val df = spark.read
        .parquet(pathToData)


      df.write.parquet("C:\\Users\\Aisultan\\Documents\\Spark\\enriched_data.parquet") //joinedDf.show()

      spark.stop()
    }
  }


