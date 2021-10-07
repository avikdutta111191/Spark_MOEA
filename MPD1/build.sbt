name := "MPD1"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "net.cilib" %% "benchmarks" % "0.1.1"
)
