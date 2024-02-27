ThisBuild / scalaVersion := "3.3.1"


lazy val root = project
  .in(file("."))
  .settings(
    name := "id2203-dataflow",
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % Versions.Pekko,
    libraryDependencies += "org.apache.pekko" %% "pekko-cluster-typed" % Versions.Pekko,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % Versions.Logback
  )
