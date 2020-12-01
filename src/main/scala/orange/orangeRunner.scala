package orange

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{asc, desc, round}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
object orangeRunner {
  def main(args: Array[String]): Unit={
    val appName = "Orange"
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //imports country data from countries_general_stats.tsv to obtain population for each country
    val country_stats = spark.read.option("multiline", "true").option("header", "true").option("sep", "\t").csv("countries_general_stats.tsv")
    // imports daily COVID data for each country from daily_stats.tsv
    val country_data = spark.read.option("multiline", "true").option("header", "true").option("sep", "\t").csv("daily_stats.tsv")

    //TODO:need to cross country_codes_a2 with itself to make a list of all potential countries that can border(many of them wont) this will work just like borders does, but using in house code
    val borders = spark.read.option("header", "true").csv("border_data.csv")

    // selects country and casts population to integer country data
    val country_pop = country_stats.select($"COUNTRY", $"POPULATION".cast("Int"))
    // selects maximum of total cases for each country from COVID data
    val country_cases = country_data
      .filter($"TOTAL_CASES" =!= "NULL")
      .withColumn("Cases", $"TOTAL_CASES".cast("Int"))
      .select($"COUNTRY", $"Cases".as("Total Cases"))
      .groupBy($"COUNTRY")
      .agg(functions.max($"Total Cases").as("TOTAL CASES"))
      .sort(functions.asc("COUNTRY"))

    // joins population data with COVID data and calculates infection rate per capita
    val infection_rate = country_cases
      .join(country_pop, country_cases("COUNTRY") === country_pop("COUNTRY"))
      .select(country_cases("COUNTRY"), $"TOTAL CASES", $"POPULATION",
        ($"TOTAL CASES" * 100 / $"POPULATION").as("infection_rate_per_capita"))
      .sort(functions.asc("COUNTRY"))
      .withColumn("infection_rate_per_capita",'infection_rate_per_capita.cast("Decimal(5,3)"))

          //create "infection_rate" from covid data directory with daily covid data
        /*val infection_rate = country_data
          .select($"location", $"population", functions.explode($"data")) //.explode will separate our nested data so we can access it
          .select($"location", $"population", $"col.date", $"col.total_cases")
          .groupBy("location", "population")
          .agg(functions.max("total_cases").as("total_cases")) //since the covid data doesn't remove cases from total_cases,
                                                                                  // using max value will give us the most recent value
          //.where($"location" === "United States")
          .select($"location", $"population", $"total_cases",
            (($"total_cases" / $"population")*100).as("infection_rate_per_capita")) //make value a percent as opposed to a decimal
          .sort(functions.desc("infection_rate_per_capita"))
          .withColumn("infection_rate_per_capita", 'infection_rate_per_capita.cast("Decimal(5,3)")) //make values easier to read
//          .withColumn("population", 'population.cast("Decimal(10,0)"))
//          .withColumn("total_cases", 'total_cases.cast("Decimal(12,0)"))*/

//TODO: once we have updated borders with inhouse code, need to adapt bcountries to use borders_dictionary as opposed to border_data.csv
    /*
    bcountries for border countries, joins the border_data.csv from https://github.com/geodatasource/country-borders with infection_rate
    to give us all the countries that border a country(home country) and the infection rate of the home country. The infection rate of the bordering country
    will be retrieved in the next join
     */
    val bcountries = borders.join(infection_rate, borders("country_name") === infection_rate("COUNTRY"), "inner")
      .select(borders("country_name"), borders("country_code"), borders("country_border_name"),
        borders("country_border_code"), infection_rate("infection_rate_per_capita").as("home_country_infection_rate_per_capita"))
//    bcountries.show(10)
    //first.printSchema()


    /*
    the result of joining our border countries with our infection data. This gives us information about the home country, its infection rate,
    bordering countries and their infection rate, and a delta value, which is the difference between these 2 rates.
     */
    val res1 = bcountries.join(infection_rate, bcountries("country_border_name") === infection_rate("COUNTRY"), "inner")
      .select(bcountries("country_name"), bcountries("country_code"), bcountries("home_country_infection_rate_per_capita").as("home_country_infection_rate_per_capita(%)"),
        bcountries("country_border_name"), bcountries("country_border_code"),
        infection_rate("infection_rate_per_capita").as("country_border_infection_rate(%)"),
        //used to calculate the delta value. round() allows us to do a calculation and round the number of decimal places, making it easier to read
       //round((bcountries("home_country_infection_rate_per_capita") - infection_rate("infection_rate_per_capita")), 2).as("delta"))
        (bcountries("home_country_infection_rate_per_capita") - infection_rate("infection_rate_per_capita")).as("delta"))
        //res.show(10)

    /*
    Part 2 of question is asking about land and water locked countries, use dataframes defined below to create dataframes with land/water locked countries
    and combine them
     */
    val landLocked = createLandLocked(spark)
//    landLocked.show(10)

    val waterLocked = createWaterLocked(bcountries, spark)
//    waterLocked.show(10)

    //combine landLocked dataframe with infection_rate to make a dataframe with the infection rate of land locked countries
    val landLockedInfRate = landLocked.join(infection_rate, landLocked("country_name") === infection_rate("COUNTRY"), "inner")
      .select(landLocked("country_name"), landLocked("country_code"),
        infection_rate("infection_rate_per_capita").as("infection_rate_per_capita(%)"))

    //combine waterLocked dataframe with infection_rate to make a dataframe with the infection rate of water locked countries
    val waterLockedInfRate = waterLocked.join(infection_rate, waterLocked("country_name") === infection_rate("COUNTRY"), "inner")
      .select(waterLocked("country_name"), waterLocked("country_code"),
        infection_rate("infection_rate_per_capita").as("infection_rate_per_capita(%)"))
//    waterLockedInfRate.show(10)



    //RESULT QUERIES

    /*
    query to give result to part 1. by using delta > 0, we can ensure no duplicates of countries in opposite directions, such as
    having both Israel-Lebanon and Lebanon-Israel.
     */
    println("Largest Difference(Delta) in Bordering Countries")
    res1.select("*")
      .where($"delta" > 0)
      .orderBy(desc("delta")) //using desc here allows us to get the largest differences at the top, and smaller differences at the end
      .show(10)

    /* Queries to give us the answer to the second part of our question. Using the Dataframes for land and water locked countries, we can do simple
    queries to give the required answers
     */
//       landLockedInfRate.select("*")
//       .orderBy(asc("infection_rate_per_capita(%)"))
//         .show(10)

    println("Highest Infection Rate in Land Locked Countries\n")
    landLockedInfRate.select("*")
      .orderBy(desc("infection_rate_per_capita(%)"))
      .show(10)

    println("Highest Infection Rate in Water Locked Countries\n")
    waterLockedInfRate.select("*")
      .orderBy(desc("infection_rate_per_capita(%)"))
      .show(10)

    // second.printSchema()

    //second.show(50)




  }

//  //TODO: adapt to accept the landlocked_countries_list(only need val landlocked since we don't really care about doubly land locked, and they're already included in the landlocked list
  //creates a dataframe of LandLocked countries from given the given landlocked.csv
  def createLandLocked(spark:SparkSession): DataFrame ={
    val landLocked =spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("landlocked.csv")
    landLocked
  }

  //uses the dataframe of our border countries and looks for there to be a NULL for border country. This means there is no land border, meaning the country is waterlocked
  def createWaterLocked(infectionFrame: DataFrame , spark:SparkSession): DataFrame ={
    import spark.implicits._
    val waterLocked = infectionFrame.filter($"country_border_code".isNull)
      .select($"country_name", $"country_code")
    waterLocked
  }

}
