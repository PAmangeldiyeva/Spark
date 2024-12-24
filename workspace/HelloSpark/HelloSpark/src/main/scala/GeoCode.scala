import java.net.URLEncoder
import scala.io.Source
import scalaj.http._

object OpenCageDataApi {
  def main(args: Array[String]): Unit = {
    val apiKey = "dabfed0b73574758854634892acea92f"
    val query = URLEncoder.encode("Savoria Dillon US", "UTF-8")
    val apiUrl = s"https://api.opencagedata.com/geocode/v1/json?key=$apiKey&q=$query"

    val response: HttpResponse[String] = Http(apiUrl).asString

    if (response.isSuccess) {
      val json = ujson.read(response.body)
      val results = json("results").arr

      for (result <- results) {
        val formatted = result("formatted").str
        val geometry = result("geometry")
        val lat = geometry("lat").num
        val lng = geometry("lng").num

        // Now we will print only the city name, country, latitude, and longitude
        println(s"City: ${result("components")("town").strOpt.getOrElse("Not available")}, " +
          s"Country: ${result("components")("country").str}, " +
          s"Latitude: $lat, Longitude: $lng")
      }
    } else {
      println(s"Failed to fetch data: ${response.statusLine}")
    }
  }
}
