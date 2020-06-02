/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.maropu

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import java.util.UUID

import scala.collection.mutable
import scala.util.control.NonFatal

import com.google.common.io.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._

object FuzzTests {

  private lazy val sqlSmithApi = SQLSmithLoader.loadApi()

  // Loads the SQLite JDBC driver
  Class.forName("org.sqlite.JDBC")

  private def debugPrint(s: String): Unit = {
    // scalastyle:off println
    println(s)
    // scalastyle:on println
  }

  private def dumpSparkCatalog(spark: SparkSession, tables: Seq[Table]): String = {
    val dbFile = new File(Utils.createTempDir(), s"dumped-spark-tables-${UUID.randomUUID}.db")
    val conninfo = s"jdbc:sqlite:${dbFile.getAbsolutePath}"

    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = DriverManager.getConnection(conninfo)
      stmt = conn.createStatement()

      tables.foreach { t =>
        val tableIdentifier = if (t.database != null) s"${t.database}.${t.name}" else t.name
        val schema = spark.table(tableIdentifier).schema

        def toSQLiteTypeName(dataType: DataType): Option[String] = dataType match {
          case ByteType | ShortType | IntegerType | LongType | _: DecimalType => Some("INTEGER")
          case FloatType | DoubleType => Some("REAL")
          case StringType => Some("TEXT")
          case tpe =>
            debugPrint(s"Cannot handle ${tpe.catalogString} in $tableIdentifier")
            None
        }

        val attrDefs = schema.flatMap { f =>
          toSQLiteTypeName(f.dataType).map(tpe => s"${f.name} $tpe")
        }
        if (attrDefs.nonEmpty) {
          val ddlStr =
            s"""
               |CREATE TABLE ${t.name} (
               |  ${attrDefs.mkString(",\n  ")}
               |);
             """.stripMargin

          debugPrint(
            s"""
               |SQLite DDL String from a Spark schema: `${schema.toDDL}`:
               |$ddlStr
             """.stripMargin)

          stmt.execute(ddlStr)
        } else {
          debugPrint(s"Valid schema not found: $tableIdentifier")
        }
      }
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    dbFile.getAbsolutePath
  }

  private def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      assert(!SQLConf.staticConfKeys.contains(k))
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val arguments = new FuzzerArguments(args)
    val outputDir = new File(arguments.outputLocation)
    if (!outputDir.exists()) {
      outputDir.mkdir()
    }

    // TODO: Makes this code independent from the Spark version
    val spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    val targetTables = {
      val tables = spark.catalog.listTables().filter(_.database == "default")
      tables.collect()
    }
    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      debugPrint(
        s"""
           |excludedRules(${rules.size}):
           |  ${rules.mkString("\n  ")}
         """.stripMargin)
      rules
    }

    def withOptimized[T](f: => T): T = {
      // Sets up all the configurations for the Catalyst optimizer
      val optConfigs = Seq(
        (SQLConf.CBO_ENABLED.key, "true"),
        (SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, "true")
      )
      withSQLConf(optConfigs: _*) {
        f
      }
    }

    val excludedRules = excludableRules.mkString(",")
    def withoutOptimized[T](f: => T): T = {
      val nonOptConfigs = Seq((SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules))
      withSQLConf(nonOptConfigs: _*) {
        f
      }
    }

    if (targetTables.nonEmpty) {
      val sqlSmithSchema = sqlSmithApi.schemaInit(
        dumpSparkCatalog(spark, targetTables), arguments.seed.toInt)
      var numStmtGenerated: Long = 0
      val errStore = mutable.Map[String, Long]()
      var numLogicErrors: Long = 0

      def dumpTestingStats(): Unit = debugPrint({
        val numErrors = errStore.values.sum
        val numValidStmts = numStmtGenerated - numErrors
        val numValidTestRatio = (numValidStmts + 0.0) / numStmtGenerated
        val errStats = errStore.toSeq.sortBy(_._2).reverse.map { case (e, n) => s"# of $e: $n" }
        s"""Fuzz testing statistics:
           |  valid test ratio: $numValidTestRatio ($numValidStmts/$numStmtGenerated)
           |  # of found logic errors: $numLogicErrors
           |  ${errStats.mkString("\n  ")}
         """.stripMargin
      })

      // Dumps the stats when doing shutdown
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = dumpTestingStats()
      })

      val blackListExceptionMsgs = Seq(
        "Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses"
      )

      try {
        val maxStmts = arguments.maxStmts.toLong
        val isInfinite = maxStmts == 0
        while (isInfinite || numStmtGenerated < maxStmts) {
          val sqlFuzz = sqlSmithApi.getSQLFuzz(sqlSmithSchema)
          numStmtGenerated += 1

          // Prints the stats periodically
          if (numStmtGenerated % 1000 == 0) {
            dumpTestingStats()
          }

          try {
            val optNum = withOptimized { spark.sql(sqlFuzz).count() }
            val nonOptNum = withoutOptimized { spark.sql(sqlFuzz).count() }
            // TODO: We need more strict checks for the answers
            if (optNum != nonOptNum) {
              Files.write(
                s"""##### query: optNum($optNum) != nonOptNum($nonOptNum) #####
                   |$sqlFuzz
                 """.stripMargin,
                new File(outputDir, s"$numLogicErrors.log"),
                StandardCharsets.UTF_8
              )
              numLogicErrors += 1
            }
          } catch {
            case NonFatal(e) =>
              // If enabled, outputs an exception log
              val exceptionName = e.getClass.getSimpleName
              if (arguments.loggingExceptionsEnabled) {
                val exceptionMsg = e.getMessage
                if (!blackListExceptionMsgs.exists(exceptionMsg.contains)) {
                  val exceptionLoggingDir = new File(outputDir, exceptionName)
                  if (!exceptionLoggingDir.exists()) {
                    exceptionLoggingDir.mkdir()
                  }
                  Files.write(
                    s"""##### ${e.getClass.getCanonicalName} #####
                       |$exceptionMsg
                       |Generated SQL Fuzz:
                       |$sqlFuzz
                     """.stripMargin,
                    new File(exceptionLoggingDir, s"$numStmtGenerated.log"),
                    StandardCharsets.UTF_8
                  )
                }
              }

              // Finally, updates exception stats
              val curVal = errStore.getOrElseUpdate(exceptionName, 0)
              errStore.update(exceptionName, curVal + 1)
            case e =>
              throw new RuntimeException(s"Fuzz testing stopped because: $e")
          }
        }
      } finally {
        sqlSmithApi.free(sqlSmithSchema)
      }
    } else {
      throw new RuntimeException({
        val catalogImpl = spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION.key)
        s"Table entries not found in the $catalogImpl catalog"
      })
    }
  }
}
