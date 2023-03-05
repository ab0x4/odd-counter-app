scalafmtOnCompile in Compile := true

organization := "com.abeseda.task"
name := "odd-counter-app"

version := "1.0.1"
crossScalaVersions := Seq("2.12.15", "2.13.8")
scalaVersion := "2.12.15"
val sparkVersion = "3.2.1"

libraryDependencies += "org.scala-lang" % "scala-library" % "version"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"
libraryDependencies += "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test"

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Duser.timezone=GMT")

updateOptions := updateOptions.value.withLatestSnapshots(false)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1" )

assemblyJarName in assembly := "odd-counter-app-uber.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
