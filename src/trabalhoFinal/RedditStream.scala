package trabalhoFinal

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode._
import scala.concurrent.duration._
import org.elasticsearch.spark.sql._

object RedditStream {
  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: RedditStream diretorio")
      System.exit(1)
    }
    val directory : String = args(0)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RedditStream")
      .getOrCreate()

    import spark.implicits._
    
    val stream = new RedditStreamProcessor(directory, 1)
    stream.startStream()
    val streamingQuery = stream.writeStream("es", "posts/sentiment")
    streamingQuery.awaitTermination()
  }
  
}