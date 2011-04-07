import java.io.File
import sbt._

class GraphBaseParentProject(info: ProjectInfo) extends ParentProject(info) {

  def doNothing = task {
    None
  }

  override def publishLocalAction = doNothing

  override def deliverLocalAction = doNothing

  override def publishAction = doNothing

  override def deliverAction = doNothing

  // Compile Settings
  object CompileSettings {
    val scalaCompileSettings =
      Seq("-deprecation",
        "-Xmigration",
        "-optimise",
        "-encoding", "utf8")

    val javaCompileSettings = Seq("-Xlint:unchecked")
  }

  // Repositories
  object Repositories {
    val SunRepository = "Sun Repository" at "http://download.java.net/maven/2/"
    val TinkerPopRepository = "TinkerPop Repository" at "http://tinkerpop.com/maven2/"
    val ClouderaRepository = "Cloudera Repository" at "https://repository.cloudera.com/content/groups/cloudera-repos/"
    val ClouderaRepository3RDParty = "Cloudera Repository Third Party" at "https://repository.cloudera.com/content/repositories/third-party/"
    val TempThrift = "Temporary Thrift Repository" at "http://people.apache.org/~rawson/repo/"
    val EaioRepository = "Eaio UUID library Repository" at "http://eaio.com/maven2"
  }

  // Dependencies
  object Dependencies {
    // Compile
    val tinkerpop = "com.tinkerpop" % "blueprints" % "0.5" intransitive
    val eaio = "com.eaio.uuid" % "uuid" % "3.2"

    // Test
    val hadoopTest = "org.apache.hadoop" % "hadoop-test" % "0.20.2-CDH3B4" % "test"
    val hbaseTest = "org.apache.hbase" % "hbase" % "0.90.1-CDH3B4" % "test" classifier "tests"
    val scalatest = "org.scalatest" % "scalatest" % "1.2" % "test"
    val junit = "junit" % "junit" % "4.5" % "test"
  }

  class GraphbaseProject(info: ProjectInfo) extends DefaultProject(info) {

    override def compileOptions = super.compileOptions ++ CompileSettings.scalaCompileSettings.map(CompileOption)

    override def javaCompileOptions = super.javaCompileOptions ++ CompileSettings.javaCompileSettings.map(JavaCompileOption)

    override def disableCrossPaths = true

    override def managedStyle = ManagedStyle.Maven

    override def defaultPublishRepository = Some(Resolver.file("Local", new File("../graphbase-pages/repository")))

    // Repositories
    lazy val SunRepository = Repositories.SunRepository
    lazy val TinkerPopRepository = Repositories.TinkerPopRepository
    lazy val ClouderaRepository = Repositories.ClouderaRepository
    lazy val ClouderaRepository3RDParty = Repositories.ClouderaRepository3RDParty
    lazy val TempThrift = Repositories.TempThrift
    lazy val EaioRepository = Repositories.EaioRepository

  }

  class BlueprintsProject(info: ProjectInfo) extends GraphbaseProject(info) {

    // Dependencies
    // Compile
    lazy val tinkerpop = Dependencies.tinkerpop
    lazy val eaio = Dependencies.eaio

    // Test
    lazy val hadoopTest = Dependencies.hadoopTest
    lazy val hbaseTest = Dependencies.hbaseTest
    lazy val scalatest = Dependencies.scalatest
    lazy val junit = Dependencies.junit

    // Compile
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
  }

  class DslProject(info: ProjectInfo) extends GraphbaseProject(info) {

  }

  // Subprojects
  lazy val blueprints = project("blueprints", "graphbase-blueprints", new BlueprintsProject(_))
  lazy val dsl        = project("dsl", "graphbase-dsl", new DslProject(_), blueprints)

}