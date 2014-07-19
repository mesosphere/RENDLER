import sbt._
import Keys._

import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

object RendlerBuild extends Build {

//////////////////////////////////////////////////////////////////////////////
// PROJECT INFO
//////////////////////////////////////////////////////////////////////////////

  val ORGANIZATION    = "mesosphere"
  val PROJECT_NAME    = "rendler"
  val PROJECT_VERSION = "0.1.0"
  val SCALA_VERSION   = "2.11.1"

//////////////////////////////////////////////////////////////////////////////
// DEPENDENCY VERSIONS
//////////////////////////////////////////////////////////////////////////////

  val MESOS_VERSION     = "0.19.1"
  val PLAY_JSON_VERSION = "2.3.1"

//////////////////////////////////////////////////////////////////////////////
// NATIVE LIBRARY PATHS
//////////////////////////////////////////////////////////////////////////////

  val pathToMesosLibs = "/usr/local/lib"

//////////////////////////////////////////////////////////////////////////////
// PROJECTS
//////////////////////////////////////////////////////////////////////////////

  lazy val root = Project(
    id = PROJECT_NAME,
    base = file("."),
    settings = rendlerSettings
  )

//////////////////////////////////////////////////////////////////////////////
// SETTINGS
//////////////////////////////////////////////////////////////////////////////

  lazy val rendlerSettings = Project.defaultSettings ++
                             basicSettings ++
                             formatSettings

  lazy val basicSettings = Seq(
    version := PROJECT_VERSION,
    organization := ORGANIZATION,
    scalaVersion := SCALA_VERSION,

    resolvers += "Typesafe Repository" at
                 "http://repo.typesafe.com/typesafe/releases/",

    libraryDependencies ++= Seq(
      "org.apache.mesos"   % "mesos"     % MESOS_VERSION,
      "com.typesafe.play" %% "play-json" % PLAY_JSON_VERSION
    ),

    scalacOptions in Compile ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature"
    ),

    javaOptions += "-Djava.library.path=%s:%s".format(
      sys.props("java.library.path"),
      pathToMesosLibs
    ),

    fork in run := true,

    fork in Test := true
  )

  lazy val formatSettings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(IndentWithTabs, false)
      .setPreference(IndentSpaces, 2)
      .setPreference(AlignParameters, false)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
      .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
      .setPreference(PreserveDanglingCloseParenthesis, true)
      .setPreference(CompactControlReadability, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(PreserveSpaceBeforeArguments, true)
      .setPreference(SpaceBeforeColon, false)
      .setPreference(SpaceInsideBrackets, false)
      .setPreference(SpaceInsideParentheses, false)
      .setPreference(SpacesWithinPatternBinders, true)
      .setPreference(FormatXml, true)
  )

}