package bigdata.streaming.utils

import java.io.File

import org.apache.commons.io.FileUtils

import scala.reflect.io.Directory

object Files {

  def removePath(path: String): Unit = {
    val dir = new Directory(new File(path))
    val status = dir.deleteRecursively()
    if (status) println(s"$path removed!")
    else println(s"$path don't removed!")
  }

  def createPath(path: String): Unit = {
    val dir = new Directory(new File(path)).createDirectory(force = true, failIfExists = true)
    if (dir.exists) println(s"$path created!")
    else println(s"$path don't created!")
  }

  def copyFiles(source: String, dest: String, sleepTime: Long = 0): List[String] = {
    val sourcePath = new File(getClass.getResource(source).getPath)
    val destPath = new File(dest)

    sourcePath.listFiles.toList.map { file =>
      val destFile = new File(s"${destPath.getAbsolutePath}/${file.getName}")
      FileUtils.copyFile(file, destFile)
      println(s"${file.getName} copied!")
      Thread.sleep(sleepTime)
      destFile.getAbsolutePath
    }
  }

}
