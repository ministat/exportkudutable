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

import collection.JavaConverters._

import org.slf4j.LoggerFactory
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession

object SparkExample {
  val logger = LoggerFactory.getLogger(SparkExample.getClass)
  def main(args: Array[String]) {
    val parser = new ArgumentsParser()
    if (!parser.parseArgs(args)) {
      return
    }
    val kuduMasters = parser.masters
    val kuduTable = parser.table
    val csvDir = parser.csvPath
    val showSample = parser.showSample

    val spark = SparkSession.builder.appName("KuduSparkExample").getOrCreate()

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

    df.write.format("csv").save(csvDir)
    println(s"Successfully exported $kuduTable to $csvDir")
  }
}
