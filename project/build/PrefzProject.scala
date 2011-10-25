import sbt._
import Process._
import com.twitter.sbt._


class prefzProject(info: ProjectInfo) extends StandardProject(info) 
with CompileThriftJava
with DefaultRepos
with SubversionPublisher
{
  
  override def filterScalaJars = false
  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.8.1"

  val gizzard = "com.twitter" % "gizzard" % "3.0.0-beta14"
  
  val asm = "asm" % "asm" % "1.5.3" % "test"
  val cglib = "cglib" % "cglib" % "2.2" % "test"
  
  val configgy  = "net.lag" % "configgy" % "1.5.2"
  
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1"
  val jmock     = "org.jmock" % "jmock" % "2.4.0"
  
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test"
  
  //################
  // val kestrel   = "net.lag" % "kestrel" % "1.2"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"

  // val ostrich   = "com.twitter" % "ostrich" % "1.1.17"
  val querulous = "com.twitter" % "querulous" % "1.1.4"
  val slf4j     = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val slf4jApi  = "org.slf4j" % "slf4j-api" % "1.5.2"
  //val smile     = "net.lag" % "smile" % "0.8.11"

  // val thrift    = "thrift" % "libthrift" % "0.2.0"
  // val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"
  val log4j     = "log4j" % "log4j" % "1.2.12"
  //#############################
  
  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public/")
}
