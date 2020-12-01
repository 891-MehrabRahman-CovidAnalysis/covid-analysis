package blue

import breeze.linalg.{DenseVector, linspace}
import breeze.plot.{Figure, plot}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable.ArrayBuffer

object RankRegions {
  def rankByMetric(spark: SparkSession, fullDS: DataFrame, metric: String, op: String = "avg"): DataFrame ={
    import spark.implicits._
    var oneTimeMetric: DataFrame = null
    op match {

      case "avg" => {
        oneTimeMetric = fullDS
          .groupBy("name")
          .agg(functions.avg(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      case "latest" => {
        val latestDate = fullDS.select(functions.max("date")).collect().map(_.getString(0))
        oneTimeMetric = fullDS
          .select("name", metric)
          .where($"date" === latestDate(0))
          .sort(functions.col(s"$metric") desc)
      }
      case _ => {
        oneTimeMetric = fullDS
          .groupBy("name")
          .agg(functions.avg(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
    }

    oneTimeMetric
  }

  def calculateMetric(spark: SparkSession, fullDS: DataFrame, metric: String, normalizer: String, numNormalizer: Double, newName: String): DataFrame ={
    var importantData = spark.emptyDataFrame
    if (normalizer != "none"){
      importantData = fullDS
        .select("name", "date", s"$metric", s"$normalizer")
        .groupBy("name", "date")
        .agg(functions.round(functions.sum(functions.col(s"$metric")/(functions.col(s"$normalizer")/numNormalizer)), 2) as newName)
        .sort(newName)
    } else {
      importantData = fullDS
        .select("name", "date", s"$metric")
        .groupBy("name", "date")
        .agg(functions.round(functions.sum(functions.col(s"$metric")/numNormalizer), 2) as newName)
        .sort(newName)
    }
    importantData
  }

  def plotMetrics(spark: SparkSession, data: DataFrame, metric: String, filename: String): Unit ={
    import spark.implicits._
    val regionList = data.select("name").distinct().collect().map(_.getString(0))
    val dates: DenseVector[Double] = DenseVector(
      data
      .select("date")
      .distinct()
      .rdd
      .map(date => DateFunc.dayInYear(date(0)).toDouble)
      .collect()
    )
    val metricPlottable: ArrayBuffer[DenseVector[Double]] = ArrayBuffer()
    for (region <- regionList) {
      metricPlottable.append(DenseVector(data
        .select(metric)
        .where($"name" === region)
        .collect
        .map(_.getDouble(0))))
    }

    val f = Figure()
    val p = f.subplot(0)
    for (ii <- 0 to regionList.length-1) {
      p += plot(dates, metricPlottable(ii), name = regionList(ii))
    }
    p.legend = true
    p.xlabel = "Days since 1st of January, 2020"
    p.ylabel = metric
    f.refresh()
    f.saveas(s"${filename}.png")
  }


}