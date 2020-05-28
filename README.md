[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/fuzz-testing-in-spark/blob/master/LICENSE)

This is an experimental SQL-aware fuzz tester for the Catalyst Optimizer in Spark; to detect corner case anomalies,
it automatically generates various patterns of SQL inputs and run them on catalog tables.

## Background

Some existing OSS communities (e.g., [SQLite](https://www.sqlite.org/testing.html#fuzztesting) [1] and
[CockroachDB](https://www.cockroachlabs.com/blog/sqlsmith-randomized-sql-testing/) [2])
already have exploited the automatic testing technique for finding the bugs that would be time-consuming to come up with by hand.
But, it has a common difficulty; there is no ground truth answer against generated inputs and it cannot naively deduce SQL correctness.

To mitigate the issue, a simple but efficient technique has been proposed in a recent research report [3].
This is named Non-Optimizing Reference Engine Construction (NoREC), a fully-automatic approach to detect optimization bugs in relational database systems.
This technique does not depend on ground truth answers, but uses output of non-optimized queries.
Specifically, it just compares output rows between optimized and non-optimized queries
on the assumption that an relational optimizer does not change them.
The proposed technique employs a black box approach to get non-optimized versions of queries;
it rewrites input queries syntactically, so it is not specific to any database system implementatons.
This technique is broadly applicable to existing databases, but it is limited to tests in WHERE clause optimizations.

Fortunately, Spark has [a configuration](https://github.com/apache/spark/blob/8bbb666622e042c1533da294ac7b504b6aaa694a/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L184-L191) to disable rules defined in the Catalyst optimizer.
This enables us to easily check output of non-optimized queries in an application side and
this tester exploits it to detect optimizer anomalies.

**This repository is work-in-progress; invalid SQL inputs for Spark SQL are generated many times (See [TODO](https://github.com/maropu/fuzz-testing-in-spark#todo)). To make them more meaningful, it needs to mutates SQL inputs based on [the Spark ANTLR grammar file](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4).**

## How to Use This Fuzzer

    // You need to set a `SPARK_HOME` env to your target Spark repository
    $ git clone https://github.com/maropu/fuzz-testing-in-spark.git
    $ cd fuzz-testing-in-spark
    $ ./bin/spark-shell

    // Defines a table in a Spark catalog
    scala> sql("CREATE TABLE t (c0 int, c1 string)")
    scala> :quit

    // Generates ten SQL queries, then run them on the target Spark.
    // If a query having different output found, it is logged in the `/tmp/fuzz-tests-output`.
    $ ./bin/run-fuzz-tests --output-location /tmp/fuzz-tests-output --max-stmts 10
    Fuzz testing statistics:
      valid test ratio: 0.1 (1/10)
      # of found logic errors: 0
      # of ParseException: 8
      # of AnalysisException: 1

## Some Notes

 - Supports OpenJDK 8 (64bit) only
 - Bundles a native [SQLSmith](https://github.com/anse1/sqlsmith) binary [4] only for Mac/x86_64 now
   - It is built by Apple clang++ v900.0.39.2 on macOS Sierra v10.12.1

## TODO

 - Generates Spark SQL acceptable SQL inputs
   - The current code just dumps all tables in a Spark catalog as a file-based SQLite database, then generates SQLite-aware queries based on the database via SQLSmith.
 - Makes the bundled native library more portable
   - It has unused dependencies to other shared libraries (e.g., libpqxx)

## References

 - [1] How SQLite Is Tested, https://www.sqlite.org/testing.html#fuzztesting.
 - [2] SQLsmith: Randomized SQL Testing in CockroachDB, https://www.cockroachlabs.com/blog/sqlsmith-randomized-sql-testing.
 - [3] Manuel Rigger and Zhendong Su, [Detecting Optimization Bugs in Database Engines via Non-Optimizing Reference Engine Construction](https://www.manuelrigger.at/publications/), Proceedings of the 28th ESEC/FSE, 2020.
 - [4] Andreas Seltenreich, Bug Squashing with SQLSmith, PGCONF.EU 2018, https://www.postgresql.eu/events/pgconfeu2018/sessions/session/2221-bug-squashing-with-sqlsmith.

## Bug Reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/fuzz-testing-in-spark/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

