import java.io.File

import sbt.Keys.{scalaSource, sourceManaged}
import sbt.{AutoPlugin, Def}
import sbt.Keys._
import sbtavrohugger.SbtAvrohugger

object AvroFormatPlugin extends AutoPlugin {
  override def requires = SbtAvrohugger

  import SbtAvrohugger.autoImport._

  override lazy val projectSettings: Seq[Def.Setting[_]] = Seq(

    sourceDirectory := new File("src/main/resources"),
    avroScalaGenerate := Seq(new File("src/main/resources"))

  )


  override lazy val buildSettings: Seq[Def.Setting[_]] = Seq(
  sourceDirectory := new File("src/main/resources"),
  avroScalaGenerate := Seq(new File("src/main/resources"))
  )

}
