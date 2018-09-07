package trabalhoFinal

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.{Dataset, Row}
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.{OutputMode, Trigger, StreamingQuery}
import java.io._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.elasticsearch.spark.sql._

class RedditStreamProcessor(var pathToFiles: String, var processingTime: Int) {
    val spark = SparkSession
        .builder
        .master("local[*]")
        .appName("RedditStream")
        .getOrCreate()
  
    import spark.implicits._
    val schema = Encoders.product[Reddit].schema
    var datasetReddit: Dataset[Row] = null
    
    def fixEncoding(text:String): String = {
        val regex = "[\\xc2-\\xf4][\\x80-\\xbf]+".r
        return regex.replaceAllIn(text, m => new String(m.group(0).getBytes("ISO-8859-1"),"UTF-8"))
    }
    
    def startStream(): Unit = {
        val reddit = spark.readStream.schema(schema)
            .option("maxFilesPerTrigger", 1)
            .json(pathToFiles.concat("/*.json"))
            .as[Reddit]
        
        val ds = reddit.select($"author", $"body", $"score", $"subreddit", $"edited", $"ups",
                                      $"controversiality", $"created_utc",
                                      $"parent_id",
                                      $"subreddit_id", $"id", $"sentiment")
        val dsCol = ds.withColumn("created_utc_1", to_date(from_unixtime($"created_utc")))
                      .drop("created_utc")
                      .withColumnRenamed("created_utc_1", "created_utc")
        dsCol.printSchema()
        val dsColStr = dsCol.withColumn("created_utc_1", $"created_utc".cast("string"))
                      .drop("created_utc")
                      .withColumnRenamed("created_utc_1", "created_utc")
        dsCol.printSchema()
        datasetReddit = dsColStr
        datasetReddit.printSchema()
    }
    
    def writeStream(format: String, queryName: String, outputPath: String = null): StreamingQuery = {
        if(format == "console"){
            val stream = datasetReddit.writeStream.format(format)
                       .option("truncate", false)
                       .trigger(Trigger.ProcessingTime(processingTime.seconds))
                       .outputMode(OutputMode.Update)
                       .queryName(queryName)
                       .start
            return stream
        }else if(format == "parquet" && outputPath != null){
            val stream = datasetReddit.writeStream
                        .format(format)
                        .option("truncate", false)
                        .trigger(Trigger.ProcessingTime(processingTime.seconds))
                        .outputMode(OutputMode.Append)
                        .option("path", pathToFiles + File.separatorChar + outputPath)
                        .option("checkpointLocation", pathToFiles + File.separatorChar +
                                                        outputPath + "checkpoint")
                        .queryName(queryName)
                        .start
            return stream
        }else if(format == "es"){
            val stream = datasetReddit.writeStream
                        .format("es")
//                        .trigger(Trigger.ProcessingTime(processingTime.seconds))
//                        .outputMode(OutputMode.Append)
                        .option("es.mapping.date.rich", "false")
                        .option("checkpointLocation", pathToFiles + File.separatorChar +
                                                        "checkpoint")
//                        .option("es.resource", "reddit/reddit_stream")
                        .start(queryName)
            return stream
        }else{
            println("supported formats: console or parquet or es")
            throw new Exception(s"supported formats: console or parquet or es")
        }
    }

}