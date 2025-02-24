// Definições de versão do Spark, Delta e Hadoop
val sparkVersion = "3.4.0"
val deltaVersion = "2.4.0"

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "fare",
      scalaVersion := "2.12.13"
    )),

    name := "nycTaxi",
    version := "0.0.1",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),

    // Configuração específica para testes
    Test / parallelExecution := false, // Evita problemas com concorrência
    Test / fork := true, // Roda testes em um processo separado

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "io.delta" %% "delta-core" % deltaVersion,
      "org.scalatest" %% "scalatest" % "3.2.10" % "test",
      "org.scalactic" %% "scalactic" % "3.2.10" % "test",
      "org.scalacheck" %% "scalacheck" % "1.15.2" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.10" % "test",
      "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.10" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "3.4.0_1.4.2" % "test", // Atualizei a versão do Spark Testing Base
      "io.github.cdimascio" % "java-dotenv" % "5.2.2",
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2",
      "com.github.scopt" %% "scopt" % "4.0.1"
    ),

    // Configuração para rodar testes no IntelliJ
    testFrameworks += new TestFramework("org.scalatest.tools.Framework"),

    // Define como o sbt executará a aplicação
    Compile / run := Defaults.runTask(
      Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner
    ).evaluated,

    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { _ => false },

    // Configuração de publicação
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    // Configuração do Assembly
    assembly / mainClass := Some("fare.nyctaxi.jobs.MainScript"), // Define a classe principal
    assembly / assemblyJarName := "nycTaxi-assembly-0.0.1.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
//      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//      case "META-INF/io.netty.versions.properties" => MergeStrategy.first
//      case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
      case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
      case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
//      case "arrow-git.properties" => MergeStrategy.first
      case PathList("META-INF", _*) => MergeStrategy.discard
//      case PathList("google", "protobuf", _*) => MergeStrategy.first
      case PathList("org", "apache", "commons", "logging", _*) => MergeStrategy.first
      case PathList("org", "slf4j", _*) => MergeStrategy.first
      case PathList("javax", "xml", "bind", _*) => MergeStrategy.first
//      case PathList("com", "fasterxml", "jackson", "core", _*) => MergeStrategy.first
//      case PathList("org", "apache", "arrow", _*) => MergeStrategy.first
//      case PathList("org", "apache", "spark", _*) => MergeStrategy.first
      case PathList("com", "github", "scopt", _*) => MergeStrategy.first
      case PathList("com", "typesafe", "config", _*) => MergeStrategy.first
      case _ => MergeStrategy.first
    }
  )
