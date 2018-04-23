package com.iamninad.serializer

import java.io.ByteArrayOutputStream

import com.iamninad.model.Employee
import com.lightbend.kafka.scala.streams.Serializer
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import scala.io.Source

class EmployeeSerializer extends Serializer[Employee]{
  override def serialize(data: Employee): Array[Byte] = {
    val schema: Schema = new Parser().parse(Source.fromURL(getClass.getClassLoader.getResource("../avro/Employee.avsc")).mkString)
    val genericRecord = new GenericData.Record(schema)

    genericRecord.put("Name", data.Name)
    genericRecord.put("Age", data.Age)

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericRecord, encoder)
    encoder.flush()
    out.close()

    out.toByteArray
  }
}
