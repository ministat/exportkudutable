// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations

package org.apache.kudu.spark.examples

import org.apache.kudu.client.CreateTableOptions

import collection.JavaConverters._
import org.slf4j.LoggerFactory
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object SparkExample {
  val logger = LoggerFactory.getLogger(SparkExample.getClass)

  /**
   * Import a CSV and upsert the content to Kudu table
   * @param spark
   * @param parser
   */
  def importCsvToKudu(spark: SparkSession, parser: ArgumentsParser): Unit = {
    val kuduMasters = parser.masters
    val kuduTable = parser.table
    val csvPath = parser.csvPath
    val df = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)
    val kuduContext = new KuduContext(kuduMasters, spark.sqlContext.sparkContext)
    import spark.implicits._
    val schema = StructType(
      List(StructField("id", IntegerType, false), // primary key
        StructField("mobile_uid", StringType, true),
        StructField("event_id", IntegerType, true),
        StructField("property_id", IntegerType, true),
        StructField("property_value", StringType, true),
        StructField("session_id", StringType, true),
        StructField("app_name", StringType, true),
        StructField("parent_id", IntegerType, true),
        StructField("imei", StringType, true),
        StructField("created_on", TimestampType, true))
    )
    if (!kuduContext.tableExists(kuduTable)) {
      kuduContext.createTable(kuduTable, schema, Seq("id"),
        new CreateTableOptions().addHashPartitions(List("id").asJava, 3))
    }
    kuduContext.upsertRows(df, kuduTable)
    val sqlDF = spark.sqlContext
      .read
      .options(Map("kudu.master" -> kuduMasters, "kudu.table" -> kuduTable))
      .format("kudu").load
    sqlDF.createOrReplaceTempView(kuduTable)
    spark.sql(s"SELECT count(*) from $kuduTable").show()
  }

  /**
   * Export a Kudu table content to csv file
   * @param spark
   * @param parser
   */
  def exportKuduToCsv(spark: SparkSession, parser: ArgumentsParser): Unit = {
    val kuduMasters = parser.masters
    val kuduTable = parser.table
    val csvDir = parser.csvPath
    val showSample = parser.showSample
    val startTime = System.nanoTime
    val df = spark.read.options(Map("kudu.master" -> kuduMasters, "kudu.table" -> kuduTable))
      .format("kudu")
      .load
    val loadingCost = "loading to table %s took %.0f milliseconds"
      .format(kuduTable, (System.nanoTime - startTime) / 1E6)
    logger.info(loadingCost)
    if (showSample) {
      val view = "ViewOf" + kuduTable
      df.createOrReplaceTempView(view)
      spark.sql(s"select * from $view").show()
      spark.sql(s"select count(*) from $view").show()
    }

    df.coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(csvDir)
    println(s"Successfully exported $kuduTable to $csvDir")
  }

  def main(args: Array[String]) {
    val parser = new ArgumentsParser()
    if (!parser.parseArgs(args)) {
      return
    }

    val spark = SparkSession.builder.appName("KuduSparkExample")
                                    .config("spark.master", "local")
                                    .getOrCreate()
    try {
      if (parser.mode.equals("E")) {
        exportKuduToCsv(spark, parser)
      } else if (parser.mode.equals("I")) {
        importCsvToKudu(spark, parser)
      }
    } finally {
      spark.close()
    }
  }
}
