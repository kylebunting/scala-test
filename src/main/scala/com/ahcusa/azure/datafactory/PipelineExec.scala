package com.ahcusa.azure.datafactory

import scala.util.matching.Regex

object PipelineExec {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Pipeline name parameter is missing.")
      return
    }

    val pipelineName = cleanPipelineName(args(0))

    pipelineName match {
      case "COPY-EDW-BRONZE-TO-GOLD-DELTA" => Pipeline.EdwBronzeToGold(args)
      case unknown => println(s"Unknown pipeline name: $unknown")
    }
  }

  private def cleanPipelineName(pipelineName: String): String = {

    val guidPattern: Regex = """([a-zA-Z0-9]{8}-([a-zA-Z0-9]{4}-){3}[a-zA-Z0-9]{12})""".r

    guidPattern.replaceFirstIn(pipelineName, "")
  }
}
