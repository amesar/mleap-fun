package org.andre.mleap.spark

import java.io.File

object CommonUtils {
  def createOutputDir(bundlePath: String) {
    if (bundlePath.startsWith("file:")) {
      val path = bundlePath.replace("file:","")
      (new File(path)).mkdirs
    } else if (bundlePath.startsWith("jar:")) {
      val path = bundlePath.replace("jar:file:","")
      (new File(path)).getParentFile.mkdirs
    } else {
      throw new Exception(s"Bad bundle URI: $bundlePath")
    }
  }
}
