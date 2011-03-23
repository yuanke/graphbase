import sbt._

class GraphBase(info: ProjectInfo) extends DefaultProject(info) {

  val SunRepository = "Sun Repository" at "http://download.java.net/maven/2/"
  val TinkerPopRepository = "TinkerPop Repository" at "http://tinkerpop.com/maven2/"
  val ClouderaRepository = "Cloudera Repository" at "https://repository.cloudera.com/content/groups/cloudera-repos/"
  val ClouderaRepository3RDParty = "Cloudera Repository Third Party" at "https://repository.cloudera.com/content/repositories/third-party/"
  val TempThrift = "Temporary Thrift Repository" at "http://people.apache.org/~rawson/repo/"
  val EaioRepository = "Eaio UUID library Repository" at "http://eaio.com/maven2"

  val tinkerpop  = "com.tinkerpop"     % "blueprints"   % "0.4"                    intransitive ()
  val hadoop     = "org.apache.hadoop" % "hadoop-core"  % "0.20.2-CDH3B4" % "compile"
  val hbase      = "org.apache.hbase"  % "hbase"        % "0.90.1-CDH3B4"
  val eaio       = "com.eaio.uuid"     % "uuid"         % "3.2"

  //Testing
  val hadoopTest = "org.apache.hadoop" % "hadoop-test"  % "0.20.2-CDH3B4" % "test"
  val hbaseTest  = "org.apache.hbase"  % "hbase"        % "0.90.1-CDH3B4" % "test" classifier "tests"
  val scalatest  = "org.scalatest"     % "scalatest"    % "1.2"           % "test"
  val junit      = "junit"             % "junit"        % "4.5"           % "test"

}