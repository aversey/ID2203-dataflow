ThisBuild / scalaVersion := "3.3.1"


lazy val root = project
  .in(file("."))
  .settings(
    name := "id2203-vt24-course-project-crdts",
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % Versions.pekko,
    libraryDependencies += "org.apache.pekko" %% "pekko-cluster-typed" % Versions.pekko,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % Versions.logback
  )
