package org.norton.SampleAPP

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException


class unitTesting extends AnyFlatSpec with SparkSessionTestWrapper with java.io.Serializable {
    private val logger = LoggerFactory.getLogger(getClass.getName)
    import spark.implicits._
    import org.apache.spark.sql._
    logger.info(" Testing started")

    //read the input file
    logger.info("Spark reading final json table that gor created in the main class")

    var finalDF: org.apache.spark.sql.DataFrame = null
    try{
      finalDF = spark.read.option("multiline", "true")
        .json("src/main/resources/finaldf_info.json")
    }catch {
      case ex: FileNotFoundException => {
        println(s"File src/main/resources/finaldf_info.json not found")
        System.exit(1)
      }
      case _ : Exception => {
        println(s"Unknown exception: Please check the main file and Final Df file tht got generate in main class ")
        System.exit(1)
      }
    }


  // Test 1 started
    logger.info(" Test 1 started ")
    behavior of "Spark table schema match "

    it should "Compare this schema with final df table " in {


      val newSchema : StructType = DataType.fromJson("""{
                                                       |  "type": "struct",
                                                       |  "fields": [
                                                       |    {
                                                       |      "name": "student_id",
                                                       |      "type": "string",
                                                       |      "nullable": true,
                                                       |      "metadata": {}
                                                       |    },{
                                                       |      "name": "school_name",
                                                       |      "type": "string",
                                                       |      "nullable": true,
                                                       |      "metadata": {}
                                                       |    },{
                                                       |      "name": "address",
                                                       |      "type": "string",
                                                       |      "nullable": true,
                                                       |      "metadata": {}
                                                       |    },{
                                                       |      "name": "student_name",
                                                       |      "type": "string",
                                                       |      "nullable": true,
                                                       |      "metadata": {}
                                                       |    },{
                                                       |      "name": "avg_test_score",
                                                       |      "type": "double",
                                                       |      "nullable": true,
                                                       |      "metadata": {}
                                                       |    },{
                                                       |      "name": "num_tests",
                                                       |      "type": "long",
                                                       |      "nullable": true,
                                                       |      "metadata": {}
                                                       |    }]
                                                       |}""".stripMargin).asInstanceOf[StructType]

      val correctData: RDD[Row]  = spark.sparkContext.parallelize(Seq(Row("S-100","Bayside High School", "13000 Oxnard St., Van Nuys, CA",
        "Kelly Kapowski", 97.2, 2)))
      val dfNew = spark.createDataFrame(correctData, newSchema)

      // Extract relevant information: name (key), type & nullability (values) of columns
      def getCleanedSchema(df: DataFrame): Map[String, (DataType, Boolean)] = {
        df.schema.map { (structField: StructField) =>
          structField.name.toLowerCase -> (structField.dataType, structField.nullable)
        }.toMap
      }
      // Compare relevant information
      def getSchemaDifference(schema1: Map[String, (DataType, Boolean)],
                              schema2: Map[String, (DataType, Boolean)]
                             ): Map[String, (Option[(DataType, Boolean)], Option[(DataType, Boolean)])] = {
        (schema1.keys ++ schema2.keys).
          map(_.toLowerCase).
          toList.distinct.
          flatMap { (columnName: String) =>
            val schema1FieldOpt: Option[(DataType, Boolean)] = schema1.get(columnName)
            val schema2FieldOpt: Option[(DataType, Boolean)] = schema2.get(columnName)

            if (schema1FieldOpt == schema2FieldOpt) None
            else Some(columnName -> (schema1FieldOpt, schema2FieldOpt))
          }.toMap
      }
      val unit_test_schema = getCleanedSchema(dfNew)
      val final_df_schema = getCleanedSchema(finalDF)

      val schema_differenc_df = getSchemaDifference(unit_test_schema, final_df_schema).toString.length
      println(getSchemaDifference(unit_test_schema, final_df_schema))

      assert(5 == schema_differenc_df)

    }
    logger.info(" Test 1 completed ")

  // Test 2 started

    logger.info(" Test 2 Started ")

    behavior of "Spark table: checking school_name "

    it should " check if school_name has just 3 schools " in {
      val schools = List("Ridgemont High School", "Bayside High School", "Baxter High School")

      val FinalDF_schoolList = finalDF.select("school_name").distinct.rdd.map(r => r(0)).collect().toList

      val lenthof  = FinalDF_schoolList.diff(schools)

      assert(0 == lenthof.length)
    }
    logger.info(" Test 2 completed ")

  // Test 3 started

    logger.info(" Test 3 starated ")

    behavior of "Spark table: checking num_tests column values not equal to null or empty"

    it should " check if num_tests column values  has no null or empty values " in {

      val finalDF_num_testsList = finalDF.select("num_tests").withColumn("IsNullUDF1", isNullEmptyFunction.isNullUDF(col("num_tests")))
        .filter(col("IsNullUDF1") === "true").rdd.map(r => r(0)).collect().toList

      assert(0 == finalDF_num_testsList.length )
    }
    logger.info(" Test 3 completed ")



  // Test 4 started

    logger.info(" Test 4 Started")


    behavior of "Spark table: checking avg_test_score column values not equal to null or empty"

    it should " check if checking avg_test_score column values has no null or empty values " in {

      val finalDF_avg_testList = finalDF.select("avg_test_score").withColumn("IsNullUDF2", isNullEmptyFunction.isNullUDF(col("avg_test_score")))
        .filter(col("IsNullUDF2") === "true").rdd.map(r => r(0)).collect().toList
      assert(0 == finalDF_avg_testList.length)
    }
    logger.info(" Test 4 completed ")

  }
