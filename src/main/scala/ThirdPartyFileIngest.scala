package com.ignitionone.idrlr


object ThirdPartyFileIngest {

  def main(args: Array[String]): Unit = {

    try {
      val proc = new LRDataProcess()
      proc.run()
    } catch {
      case e: Exception =>
        println("=============================================================")
        println(e.printStackTrace())
        println("=============================================================")
    }
  }
}
