package com.iamninad.model

case class BusinessEvent(eventType: String, events: Map[String, Array[Byte]])
