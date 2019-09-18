import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object TemplateApplication {

  case class Record
  (
    gkgRecordId: String,
    date: Timestamp,
    sourceCollectionIdentifier: Integer,
    sourceCommonName: String,
    documentIdentifier: String,
    counts: String,
    v2Counts: String,
    themes: String,
    v2Themes: String,
    locations: String,
    v2Locations: String,
    persons: String,
    v2Persons: String,
    organizations: String,
    v2Organizations: String,
    v2Tone: String,
    dates: String,
    gCAM: String,
    sharingImage: String,
    relatedImages: String,
    socialImageEmbeds: String,
    socialVideoEmbeds: String,
    quotations: String,
    allNames: String,
    amounts: String,
    translationInfo: String,
    extras: String
  )


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Scala Application template").config("spark.master", "local[*]").getOrCreate()

    val schema = StructType(
      Array(
        StructField("GKGRECORDID", StringType, nullable = true),
        StructField("DATE", TimestampType, nullable = true),
        StructField("SourceCollectionIdentifier", IntegerType, nullable = true),
        StructField("SourceCommonName", StringType, nullable = true),
        StructField("DocumentIdentifier", StringType, nullable = true),
        StructField("Counts", StringType, nullable = true),
        StructField("V2Counts", StringType, nullable = true),
        StructField("Themes", StringType, nullable = true),
        StructField("V2Themes", StringType, nullable = true),
        StructField("Locations", StringType, nullable = true),
        StructField("V2Locations", StringType, nullable = true),
        StructField("Persons", StringType, nullable = true),
        StructField("V2Persons", StringType, nullable = true),
        StructField("Organizations", StringType, nullable = true),
        StructField("V2Organizations", StringType, nullable = true),
        StructField("V2Tone", StringType, nullable = true),
        StructField("Dates", StringType, nullable = true),
        StructField("GCAM", StringType, nullable = true),
        StructField("SharingImage", StringType, nullable = true),
        StructField("RelatedImages", StringType, nullable = true),
        StructField("SocialImageEmbeds", StringType, nullable = true),
        StructField("SocialVideoEmbeds", StringType, nullable = true),
        StructField("Quotations", StringType, nullable = true),
        StructField("AllNames", StringType, nullable = true),
        StructField("Amounts", StringType, nullable = true),
        StructField("TranslationInfo", StringType, nullable = true),
        StructField("Extras", StringType, nullable = true)
      ))

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._

    val fs = FileSystem.get(new Configuration())

    val files = fs.listFiles(new Path("./data/segment"), true)

    val filePaths = new ListBuffer[String]

    while (files.hasNext) {
      val file = files.next
      filePaths += file.getPath.toString
    }


    val ds = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .option("timestampFormat", "YYYYMMDDhhmmss")
      .csv(filePaths.head)
      .as[Record]

    ds.collect.foreach(println)

    spark.stop()
  }
}
