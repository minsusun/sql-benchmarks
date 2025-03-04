package com.ibm.crail.benchmarks

trait LogTrait {
  private var lastStepTime = System.nanoTime();
  private var logString = ""

  def step(StepName: String): Unit = {
    val now = System.nanoTime();
    logString += "\n\t\t\t\tStep '" + StepName + s"': ${(now - lastStepTime)/1000000} ms"
    lastStepTime = now
  }

  def concatLog(externalLogString: String): Unit = {
    logString += externalLogString
  }

  def logToString: String = logString;
}
