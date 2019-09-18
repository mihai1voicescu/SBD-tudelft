import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import System.nanoTime

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Benchmark(var last: Long = System.nanoTime) {
  def mark(): Long = {
    val current = System.nanoTime

    val diff = current - last

    last = current

    diff
  }

  def markPrint(str: String = ""): Long = {
    val res = mark()

    val humanSeconds: Float = (res / 1000000L) / 1000f // 10**6

    println(f"$str%10s: $humanSeconds%3.5f")

    res
  }
}

object TemplateApplication {
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

  case class Mentions
  (
    id: String,
    counts: Int
  )

  def mapper(str: String): (String, Int) = {
    (str, 1)
  }

  def mapper1(str: String): (String, Int) = {
    val tokens = str.split(",")

    val counts = Integer.parseInt(tokens(1))

    (tokens(0), counts)
  }


  def mapperRDD(str: String): TraversableOnce[(String, Int)] = {
    val fields = str.split("\t")

    if ((fields.length > 23) && (fields(23) != ""))
      return fields(23).split(";").map(mapper1)


    List()
  }


  def main(args: Array[String]) {
    val fs = FileSystem.get(new Configuration())

    val files = fs.listFiles(new Path("./data/segment"), true)

    val filePaths = new ListBuffer[String]

    while (files.hasNext) {
      val file = files.next
      filePaths += file.getPath.toString
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    if (args.isEmpty) {
      val results = new ListBuffer[(String, Long, Any)]()

      results += rdd(filePaths)
      results += datasetLoadedWithCsv(filePaths)

    }
    else args(0) match {
      case "rdd" => println(rdd(filePaths)._3)
      case "dataset" => println(datasetLoadedWithCsv(filePaths)._3)
    }
  }

  def datasetLoadedWithCsv(filePaths: ListBuffer[String]): (String, Long, Array[Row]) = {

    val spark = SparkSession.builder.appName("Spark Scala Application template").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._

    val benchmark = new Benchmark()
    val results =
      spark.read
        .schema(schema)
        .option("delimiter", "\t")
        .option("timestampFormat", "YYYYMMDDhhmmss")
        .csv(filePaths: _*)
        .as[Record]
        .filter(_.allNames != null)
        .flatMap(_.allNames.split(";"))
        .filter(!_.matches("^.*ParentCategory,.*$"))
        .map(mapper1)
        .groupBy("_1")
        .sum("_2")
        .sort(desc("sum(_2)"))
        .take(10)
    val time = benchmark.markPrint("Dataset")
    spark.stop()


    ("Dataset", time, results)
  }

  def rdd(filePaths: ListBuffer[String]): (String, Long, Array[(String, Int)]) = {
    val spark = SparkSession.builder.appName("Spark Scala Application template").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._

    val benchmark = new Benchmark()
    val results =

      spark.sparkContext.textFile("./data/segment")
        .flatMap(mapperRDD)
        .filter(!_._1.matches("^.*ParentCategory$"))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(10)

    val time = benchmark.markPrint("RDD")

    spark.stop()

    ("RDD", time, results)
  }


  def rddWithConversion(filePaths: ListBuffer[String]): (String, Long, Array[(String, Int)]) = {
    val spark = SparkSession.builder.appName("Spark Scala Application template").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._

    val benchmark = new Benchmark()
    val results =

      spark.read
        .schema(schema)
        .option("delimiter", "\t")
        .option("timestampFormat", "YYYYMMDDhhmmss")
        .csv(filePaths: _*)
        .as[Record].rdd
        .filter(_.allNames != null)
        .flatMap(_.allNames.split(";"))
        .filter(!_.matches("^.*ParentCategory,.*$"))
        .map(mapper1)
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(10)

    val time = benchmark.markPrint("rddWithConversion")

    spark.stop()

    ("rddWithConversion", time, results)
  }
}
