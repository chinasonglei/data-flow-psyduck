package com.sxkj.utils


import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties



object PropertiesUtils {

  val confDirectory = "/tmp/psyduck.properties"

  def loadPointProperties(key: String): String = {
    val properties = new Properties()
    val inBur = new BufferedInputStream(new FileInputStream(confDirectory))
    properties.load(inBur)
    inBur.close()
    properties.getProperty(key)
  }

}
