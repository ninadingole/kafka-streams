package com.iamninad.avro.generator

import java.io.File

class EmployeeGenerator extends App {

  val schema = new File(this.getClass.getClassLoader.getResource("avro/Employee.avsc").getPath)


}
