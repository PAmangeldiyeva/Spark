import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import java.net.URLEncoder
import scala.io.Source
import scalaj.http._
import scala.util.parsing.json.JSON
import ch.hsr.geohash.GeoHash

object YourSparkJob {

  // A function for calling the API and getting coordinates
  def callApi(franchiseName: String, country: String, city: String, apiKey: String): (Double, Double) = {
    val query = URLEncoder.encode(s"$franchiseName, $city, $country", "UTF-8")
    val apiUrl = s"https://api.opencagedata.com/geocode/v1/json?key=$apiKey&q=$query"
    val response = Http(apiUrl).asString.body

    JSON.parseFull(response) match {
      case Some(data: Map[String, Any]) =>
        val results = data("results").asInstanceOf[List[Map[String, Any]]]
        if (results.nonEmpty) {
          val geometry = results.head("geometry").asInstanceOf[Map[String, Any]]
          val lat = geometry("lat").asInstanceOf[Double]
          val lng = geometry("lng").asInstanceOf[Double]
          (lat, lng)
        } else (0.0, 0.0)
      case None => (0.0, 0.0)
    }
  }

  // Function for generating a geohash
  def geohash(lat: Double, lng: Double): String = {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 4) // Предполагая точность 4 символа
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Filter and Update Missing LatLng with Geohash")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val apiKey = "dabfed0b73574758854634892acea92f"

    // Reading the original DataFrame
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\Aisultan\\Documents\\Spark\\restaurant_csv")

    // Defining a UDF for an API call
    val getLatLngUdf = udf(callApi _)
    val geohashUdf = udf(geohash _)

    // Updating coordinates and adding a geohash column
    val updatedLatLngDf = df.filter($"lat".isNull || $"lng".isNull || $"lat" === 0 || $"lng" === 0)
      .withColumn("coordinates", getLatLngUdf($"franchise_name", $"country", $"city", lit(apiKey)))
      .withColumn("lat", $"coordinates._1")
      .withColumn("lng", $"coordinates._2")
      .drop("coordinates")
      .withColumn("geohash", geohashUdf($"lat", $"lng"))

    // Combining the updated coordinates and geohash with the original DataFrame
    val finalDf = df
      .join(updatedLatLngDf, Seq("id"), "left_outer")
      .select(df("*"), updatedLatLngDf("geohash"))

    // Reading weather data
    val weatherDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("C:\\Users\\Aisultan\\Documents\\Spark\\WeatherDS\\weather")
      .withColumn("geohash", geohashUdf($"lat", $"lng"))

    // Connecting restaurant data with weather data
    val joinedDf = finalDf
      .join(weatherDf, Seq("geohash"), "left_outer")

    // Writing down the result
    joinedDf.write.parquet("C:\\Users\\Aisultan\\Documents\\Spark\\enriched_data.parquet") //joinedDf.show()

    spark.stop()
  }
}
