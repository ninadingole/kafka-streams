package com.iamninad.deserializer

import com.iamninad.model.Employee
import com.lightbend.kafka.scala.streams.Deserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

import scala.io.Source

class EmployeeDeserializer extends Deserializer[Employee]{
  override def deserialize(data: Array[Byte]): Option[Employee] = {

    val schemaSource = Source.fromURI(this.getClass.getClassLoader.getResource("avro/Employee.avsc").toURI)

    val schema = new Schema.Parser().parse(schemaSource.mkString)

    val reader = new SpecificDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(data, null)
    val employee: GenericRecord = reader.read(null, decoder)
    if(employee != null){
      Option(Employee(employee.get("Name").toString, employee.get("Age").toString.toInt))
    }else{
      Option.empty[Employee]
    }
  }
}
