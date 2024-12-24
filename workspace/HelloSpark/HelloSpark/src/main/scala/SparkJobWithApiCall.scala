import org.apache.spark.sql.SparkSession
import java.net.URLEncoder
import scala.io.Source
import scalaj.http._
import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions._

object SparkJobWithApiCall {
  def callApi(franchiseName: String, country: String, city: String, apiKey: String): (Double, Double) = {
    val query = URLEncoder.encode(s"$franchiseName, $city, $country", "UTF-8")
    val apiUrl = s"https://api.opencagedata.com/geocode/v1/json?key=$apiKey&q=$query"
    val response = Http(apiUrl).asString

    if (response.isSuccess) {
      val json = JSON.parseFull(response.body)
      json match {
        case Some(map: Map[String, Any]) =>
          val results = map("results").asInstanceOf[List[Map[String, Any]]]
          if (results.nonEmpty) {
            val geometry = results.head("geometry").asInstanceOf[Map[String, Double]]
            val lat = geometry("lat")
            val lng = geometry("lng")
            (lat, lng)
          } else {
            (0.0, 0.0) // We return (0.0, 0.0) for cases when the API does not find coordinates
          }
        case None => (0.0, 0.0) // The same is true for invalid JSON
      }
    } else {
      println(s"Failed to fetch data: ${response.statusLine}")
      (0.0, 0.0) // And in case of an API error
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Update Missing LatLng with API")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val necessaryFieldsDf = Seq(
      ("Savoria", "US", "Dillon")
    ).toDF("franchise_name", "country", "city")

    val apiKey = "dabfed0b73574758854634892acea92f"

    // Adding new coordinates to a DataFrame using the API
    val updatedDf = necessaryFieldsDf.map(row => {
      val franchiseName = row.getAs[String]("franchise_name")
      val country = row.getAs[String]("country")
      val city = row.getAs[String]("city")

      val (lat, lng) = callApi(franchiseName, country, city, apiKey)

      (franchiseName, country, city, lat, lng)
    }).toDF("franchise_name", "country", "city", "lat", "lng")

    updatedDf.show()


    spark.stop()
  }
}
