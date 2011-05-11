/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File
import sbt._
import xml.transform.{RuleTransformer, RewriteRule}
import xml.{Elem, Node}

class GraphBaseParentProject(info: ProjectInfo) extends ParentProject(info) {

  //Dependencies versions
  val hadoopVersion = "0.20.2-cdh3u0"
  val hbaseVersion = "0.90.1-cdh3u0"
  val zookeeperVersion = "3.3.3-cdh3u0"
  val blueprintsVersion = "0.6"
  val blueprintsTestVersion = "0.5"
  val gremlinVersion = "0.9"


  def doNothing() = task {
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
    val scalaToolsSnapshots = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots"
    val scalaToolsReleases = "Scala Tools Releases" at "http://scala-tools.org/repo-releases"
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
    val blueprints = "com.tinkerpop.blueprints" % "blueprints-core" % blueprintsVersion intransitive
    val zookeeper = "org.apache.zookeeper" % "zookeeper" % zookeeperVersion
    val eaio = "com.eaio.uuid" % "uuid" % "3.2"
    val commonsLang = "commons-lang" % "commons-lang" % "2.6"

    // Test
    val hadoopTest = "org.apache.hadoop" % "hadoop-test" % hadoopVersion % "test"
    val hbaseTest = "org.apache.hbase" % "hbase" % hbaseVersion % "test" classifier "tests"
    val scalatest = "org.scalatest" % "scalatest" % "1.2" % "test"
    val junit = "junit" % "junit" % "4.5" % "test"
    val junitInterface = "com.novocode" % "junit-interface" % blueprintsVersion % "test->default"
    val blueprintsTest = "com.tinkerpop" % "blueprints-tests" % blueprintsTestVersion % "test" intransitive

    // Compile & Test
    val ivyXML =
      <dependencies>
        <dependency org="com.tinkerpop" name="gremlin" rev={gremlinVersion} conf="test">
            <exclude org="org.openrdf.sesame"/>
            <exclude org="net.fortytwo"/>
            <exclude module="blueprints-sail-graph"/>
        </dependency>
        <dependency org="org.apache.hadoop" name="hadoop-core" rev={hadoopVersion} conf="compile">
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
        <dependency org="org.apache.hbase" name="hbase" rev={hbaseVersion} conf="compile">
            <exclude module="avro"/>
            <exclude module="commons-lang"/>
            <exclude module="guava"/>
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

  }

  class GraphbaseProject(info: ProjectInfo) extends DefaultProject(info) {

    override def compileOptions = super.compileOptions ++ CompileSettings.scalaCompileSettings.map(CompileOption)

    override def javaCompileOptions = super.javaCompileOptions ++ CompileSettings.javaCompileSettings.map(JavaCompileOption)

    override def disableCrossPaths = true

    override def managedStyle = ManagedStyle.Maven

    override def defaultPublishRepository = Some(Resolver.file("Local", new File("../graphbase-pages/repository")))

    // Repositories
    lazy val scalaToolsSnapshots = Repositories.scalaToolsSnapshots
    lazy val scalaToolsReleases = Repositories.scalaToolsReleases
    lazy val SunRepository = Repositories.SunRepository
    lazy val TinkerPopRepository = Repositories.TinkerPopRepository
    lazy val ClouderaRepository = Repositories.ClouderaRepository
    lazy val ClouderaRepository3RDParty = Repositories.ClouderaRepository3RDParty
    lazy val TempThrift = Repositories.TempThrift
    lazy val EaioRepository = Repositories.EaioRepository

    override def pomExtra =
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
        </license>
      </licenses>

  }

  class BlueprintsProject(info: ProjectInfo) extends GraphbaseProject(info) {

    // Dependencies
    // Compile
    lazy val blueprints = Dependencies.blueprints
    lazy val eaio = Dependencies.eaio
    lazy val zookeeper = Dependencies.zookeeper
    lazy val commonsLang = Dependencies.commonsLang

    // Test
    lazy val hadoopTest = Dependencies.hadoopTest
    lazy val hbaseTest = Dependencies.hbaseTest
    lazy val scalatest = Dependencies.scalatest
    lazy val junit = Dependencies.junit
    lazy val junitInterface = Dependencies.junitInterface
    lazy val blueprintsTest = Dependencies.blueprintsTest

    // Compile
    override def ivyXML = Dependencies.ivyXML

    object AddHadoopAndHbaseExclusions extends RewriteRule {

      override def transform(n: Node): Seq[Node] = {
        n match {
          case Elem(prefix, "dependency", attribs, scope, children@_ *) => {
            if (children(3) == <artifactId>hadoop-core</artifactId>)
              <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-core</artifactId>
                <version>{hadoopVersion}</version>
                <exclusions>
                  <exclusion>
                    <groupId>commons-cli</groupId>
                    <artifactId>commons-cli</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>xmlenc</groupId>
                    <artifactId>xmlenc</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-httpclient</groupId>
                    <artifactId>commons-httpclient</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-codec</groupId>
                    <artifactId>commons-codec</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-net</groupId>
                    <artifactId>commons-net</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jetty</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jetty-util</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>tomcat</groupId>
                    <artifactId>jasper-runtime</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>tomcat</groupId>
                    <artifactId>jasper-compiler</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jsp-api-2.1</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jsp-2.1</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-el</groupId>
                    <artifactId>commons-el</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>net.java.dev.jets3t</groupId>
                    <artifactId>jets3t</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>servlet-api-2.5</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>javax.servlet.jsp</groupId>
                    <artifactId>jsp-api</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>javax.servlet</groupId>
                    <artifactId>servlet-api</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>net.sf.kosmosfs</groupId>
                    <artifactId>kfs</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>hsqldb</groupId>
                    <artifactId>hsqldb</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>oro</groupId>
                    <artifactId>oro</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.eclipse.jdt</groupId>
                    <artifactId>core</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.codehaus.jackson</groupId>
                    <artifactId>jackson-core-asl</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.codehaus.jackson</groupId>
                    <artifactId>jackson-mapper-asl</artifactId>
                  </exclusion>
                </exclusions>
              </dependency>
            else
            if (children(3) ==
              <artifactId>hbase</artifactId>
            )
              <dependency>
                <groupId>org.apache.hbase</groupId>
                <artifactId>hbase</artifactId>
                <version>{hbaseVersion}</version>
                <exclusions>
                  <exclusion>
                    <groupId>org.apache.avro</groupId>
                    <artifactId>avro</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-lang</groupId>
                    <artifactId>commons-lang</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>com.google.guava</groupId>
                    <artifactId>guava</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.apache.hadoop</groupId>
                    <artifactId>avro</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.apache.zookeeper</groupId>
                    <artifactId>zookeeper</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.apache.thrift</groupId>
                    <artifactId>thrift</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.jruby</groupId>
                    <artifactId>jruby-complete</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-api</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>com.google.protobuf</groupId>
                    <artifactId>protobuf-java</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>com.sun.jersey</groupId>
                    <artifactId>jersey-core</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>com.sun.jersey</groupId>
                    <artifactId>jersey-json</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>com.sun.jersey</groupId>
                    <artifactId>jersey-server</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>javax.ws.rs</groupId>
                    <artifactId>jsr311-api</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>javax.xml.bind</groupId>
                    <artifactId>jaxb-api</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>stax</groupId>
                    <artifactId>stax-api</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-cli</groupId>
                    <artifactId>commons-cli</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-codec</groupId>
                    <artifactId>commons-codec</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>commons-httpclient</groupId>
                    <artifactId>commons-httpclient</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jetty</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jetty-util</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jsp-2.1</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jsp-api-2.1</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>servlet-api-2.5</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>tomcat</groupId>
                    <artifactId>jasper-compiler</artifactId>
                  </exclusion>
                  <exclusion>
                    <groupId>tomcat</groupId>
                    <artifactId>jasper-runtime</artifactId>
                  </exclusion>
                </exclusions>
              </dependency>
            else
            if (children(3) ==
              <artifactId>zookeeper</artifactId>
            )
              <dependency>
                <groupId>org.apache.zookeeper</groupId>
                <artifactId>zookeeper</artifactId>
                <version>{zookeeperVersion}</version>
                <exclusions>
                  <exclusion>
                    <groupId>jline</groupId>
                    <artifactId>jline</artifactId>
                  </exclusion>
                </exclusions>
              </dependency>
            else
              n
          }
          case _ => n
        }
      }
    }

    val ruleTransformer = new RuleTransformer(AddHadoopAndHbaseExclusions)

    override def pomPostProcess(pom: Node): Node = {
      ruleTransformer(pom)
    }
  }

  // Subprojects
  lazy val blueprints = project("blueprints", "graphbase-blueprints", new BlueprintsProject(_))

}