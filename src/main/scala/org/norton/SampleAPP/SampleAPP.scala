package org.norton.SampleAPP


import java.io.{FileNotFoundException, IOException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, LogManager, Logger}

import scala.util.parsing.json.{JSONObject, JSONArray}

object SampleAPP {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args:Array[String]): Unit = {
    try{
      logger.info("main method started")
      val arg_length = args.length
      if (arg_length == 0){
        logger.warn("No arguments passed")
        System.exit(1)
      }
      val env_name = args(0)
      logger.info("The environment is "+env_name)
    }catch{
      case e: IOException => println("Had an IOException trying to read that file")
    }

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark")
      .getOrCreate()
    val sc = spark.sparkContext


    spark.sparkContext.setLogLevel("ERROR")
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    // Read the files from resources
    var studentInfo_df: org.apache.spark.sql.DataFrame = null
    var testScores_df: org.apache.spark.sql.DataFrame = null
    try {
      studentInfo_df = spark.read.option("multiline", "true")
        .json("src/main/resources/student_info.json")
    }catch {
      case _: FileNotFoundException => {
        println(s"File src/main/resources/student_info.json not found")
        System.exit(1)
      }}

      try {
          testScores_df = spark.read.option("multiline", "true")
            .json("src/main/resources/test_scores.json")
        }catch {
          case _: FileNotFoundException => {
            println(s"File src/main/resources/test_scores.json not found")
            System.exit(1)
          }}


    logger.info("Spark read data as two dataframes")

    // Here removing any duplicates do a broadcast join. We really don't need broadcast join as data set is too small from two json's
    val dropDups_df = testScores_df.dropDuplicates().withColumnRenamed("student_id", "std_id")
    print("We don't need brodcast Join here: Just doing to differentiate as below we do another join-- We don't need this (you can comment this line and uncomment below line of code ")
    // If you don't have much compute power please remove use "//" and use bellow code
    val joined_df = dropDups_df.join(broadcast(studentInfo_df),
      dropDups_df("std_id") <=> studentInfo_df("student_id")).drop("std_id")

    //val joined_df = dropDups_df.withColumnRenamed("std_id", "student_id").join(studentInfo_df, "student_id")

    // Filtering test_date col not having values on 2020-04-05
    val filtered_df = joined_df.filter(col("test_date") =!= "2020-04-05")
    logger.info("filtering the data is complete")

    // Mapping school_id with school names
    val mapping_schoolId_df = filtered_df.withColumn("school_id", when(col("school_id") === "1", lit("Bayside High School"))
      .when(col("school_id") === "2", lit("Ridgemont High School"))
      .when(col("school_id")=== "3", lit("Baxter High School"))
      .otherwise(lit("unknown")))
    logger.info("mapping school's is complete")

    //Here data got grouped by student id and agg average test scores with counts on num_tests
    val agg_df = mapping_schoolId_df.groupBy("student_id").agg(avg("test_score") as "avg_test_score", count(lit(1)))
      .withColumnRenamed("count(1)", "num_tests")

    logger.info("Agg is complete")

    // Here Finally joined and removed and renamed columns that are required as per business requirements
    val final_df = mapping_schoolId_df.join(agg_df, "student_id").withColumnRenamed("name", "student_name")
      .withColumnRenamed("school_id", "school_name")
      .drop("test_date", "test_id", "test_score")
    logger.info("Final saving the data")

    // Finally data got saved into src/main/resources/finaldf_info.json
    final_df.show(false)
    final_df.printSchema()
    final_df.createOrReplaceTempView("student_report")
    final_df.write.json("students_report")
    final_df.write.json("src/main/resources/finaldf_info.json")

  }
}

