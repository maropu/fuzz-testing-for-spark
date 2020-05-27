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

import scala.util.Try

class FuzzerArguments(val args: Array[String]) {
  var outputLocation: String = null
  var maxStmts = "10"
  var seed = "0"

  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while(args.nonEmpty) {
      args match {
        case ("--output-location") :: value :: tail =>
          outputLocation = value
          args = tail

        case ("--max-stmts") :: value :: tail =>
          maxStmts = value
          args = tail

        case ("--seed") :: value :: tail =>
          seed = value
          args = tail

        case ("--help") :: tail =>
          printUsageAndExit(0)

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |Usage: spark-submit --class <this class> --conf key=value <fuzz testing jar> [Options]
      |Options:
      |  --output-location [STR]   Path to an output location
      |  --max-stmts [NUM]         Terminates after generating this many queries (default: 10)
      |  --seed [NUM]              seed RNG with specified value (default: 0)
      |
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (outputLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify an output location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (Try(maxStmts.toLong).getOrElse(-1L) < 0) {
      // scalastyle:off println
      System.err.println("Number of max statements must be greater than or equal to 0")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    Try(seed.toInt).getOrElse {
      // scalastyle:off println
      System.err.println("Seed must be a number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
