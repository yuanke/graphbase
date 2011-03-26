import java.io.File
import sbt._

class GraphBase(info: ProjectInfo) extends DefaultProject(info) {

  val SunRepository = "Sun Repository" at "http://download.java.net/maven/2/"
  val TinkerPopRepository = "TinkerPop Repository" at "http://tinkerpop.com/maven2/"
  val ClouderaRepository = "Cloudera Repository" at "https://repository.cloudera.com/content/groups/cloudera-repos/"
  val ClouderaRepository3RDParty = "Cloudera Repository Third Party" at "https://repository.cloudera.com/content/repositories/third-party/"
  val TempThrift = "Temporary Thrift Repository" at "http://people.apache.org/~rawson/repo/"
  val EaioRepository = "Eaio UUID library Repository" at "http://eaio.com/maven2"

  val tinkerpop = "com.tinkerpop" % "blueprints" % "0.4" intransitive
  val eaio = "com.eaio.uuid" % "uuid" % "3.2"

  //Testing
  val hadoopTest = "org.apache.hadoop" % "hadoop-test" % "0.20.2-CDH3B4" % "test"
  val hbaseTest = "org.apache.hbase" % "hbase" % "0.90.1-CDH3B4" % "test" classifier "tests"
  val scalatest = "org.scalatest" % "scalatest" % "1.2" % "test"
  val junit = "junit" % "junit" % "4.5" % "test"

  override def ivyXML =
    <dependencies>
      <dependency org="org.apache.hadoop" name="hadoop-core" rev="0.20.2-CDH3B4" conf="compile">
          <exclude module="commons-cli"/>
          <exclude module="xmlenc"/>
          <exclude module="commons-httpclient"/>
          <exclude module="commons-codec"/>
          <exclude module="commons-net"/>
          <exclude module="commons-el"/>
          <exclude module="jetty"/>
          <exclude module="jetty-util"/>
          <exclude module="jasper-runtime"/>
          <exclude module="jasper-compiler"/>
          <exclude module="jsp-api-2.1"/>
          <exclude module="jsp-2.1"/>
          <exclude module="jets3t"/>
          <exclude module="servlet-api-2.5"/>
          <exclude module="kfs"/>
          <exclude module="hsqldb"/>
          <exclude module="oro"/>
          <exclude module="core"/>
          <exclude module="kfs"/>
          <exclude module="servlet-api"/>
          <exclude module="jsp-api"/>
          <exclude module="jackson-core-asl"/>
          <exclude module="jackson-mapper-asl"/>
      </dependency>
      <dependency org="org.apache.hbase" name="hbase" rev="0.90.1-CDH3B4" conf="compile">
          <exclude module="avro"/>
          <exclude module="commons-lang"/>
          <exclude module="guava"/>
          <exclude module="commons-logging"/>
          <exclude module="commons-el"/>
          <exclude module="log4j"/>
          <exclude module="zookeeper"/>
          <exclude module="thrift"/>
          <exclude module="jruby-complete"/>
          <exclude module="slf4j-api"/>
          <exclude module="slf4j-log4j12"/>
          <exclude module="protobuf-java"/>
          <exclude module="jersey-core"/>
          <exclude module="jersey-json"/>
          <exclude module="jersey-server"/>
          <exclude module="jsr311-api"/>
          <exclude module="jaxb-api"/>
          <exclude module="stax-api"/>
          <exclude module="commons-cli"/>
          <exclude module="commons-codec"/>
          <exclude module="commons-httpclient"/>
          <exclude module="jetty"/>
          <exclude module="jetty-util"/>
          <exclude module="jsp-2.1"/>
          <exclude module="jsp-api-2.1"/>
          <exclude module="servlet-api-2.5"/>
          <exclude module="jasper-compiler"/>
          <exclude module="jasper-runtime"/>
      </dependency>
    </dependencies>

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def managedStyle = ManagedStyle.Maven

  override def defaultPublishRepository = Some(Resolver.file("Local", new File("../graphbase-pages/repository")))
}