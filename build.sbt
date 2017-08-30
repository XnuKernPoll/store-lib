scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
)


libraryDependencies ++= Seq (
  
  "org.scodec" % "scodec-core_2.11" % "1.10.3",

  "com.twitter" % "finagle-http_2.11" % "6.43.0",

  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"

)


fork in Test := true



