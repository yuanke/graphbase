package it.davidgreco.graphbase.tools.graphgenerator

trait Message
case class CreateSlaves(numSlaves: Int)
case class ConnectSlaves(quorum: String, port: String, name: String)
case class GenerateRange(from: Int, to: Int) extends Message
case class GenerateGraph(numVerticesPerSlave: Int)
case class ShutdownSlaves()