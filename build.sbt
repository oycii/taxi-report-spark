name := "taxi-report-spark"

version := "0.1"

scalaVersion := "2.12.10"


val sparkVersion = "3.1.0"
val scalaTestVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",

  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.3" ,
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0",

  "org.scalikejdbc"         %% "scalikejdbc"                          % "3.5.0"   % Test,
  "org.scalikejdbc"         %% "scalikejdbc-test"                     % "3.5.0"   % Test,
  "com.dimafeng"            %% "testcontainers-scala-postgresql"      % "0.38.7"  % Test,
  "com.dimafeng"            %% "testcontainers-scala-scalatest"       % "0.38.7"  % Test,
  "org.flywaydb"            % "flyway-core"                         % "7.3.2",
  "org.postgresql"          % "postgresql"                            % "42.2.2" ,
  "ch.qos.logback" % "logback-classic" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
)