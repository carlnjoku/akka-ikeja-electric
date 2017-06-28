name := "akka-quickstart-scala"

version := "1.0"

scalaVersion := "2.11.8"

lazy val akkaVersion = "2.5.2"

lazy val akka_http_version="10.0.7"

//resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  //"org.apache.spark" % "spark-mllib_2.10" % "2.0.0" ,
  "mysql" % "mysql-connector-java" % "5.1.24",

  "com.typesafe.akka" %% "akka-actor" % "2.5.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.2",

  "com.typesafe.akka" %% "akka-http"         % akka_http_version,
  "com.typesafe.akka" %% "akka-http-xml"     % akka_http_version,
  "com.typesafe.akka" %% "akka-stream"       % akkaVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % akka_http_version % Test,
  "org.scalatest"     %% "scalatest"         % "3.0.1"         % Test,

  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3",
  "net.liftweb" % "lift-json_2.11" % "2.6.2"
)
