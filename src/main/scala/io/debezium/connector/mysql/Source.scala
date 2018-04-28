package io.debezium.connector.mysql

case class Source(version: Option[String] = None, name: String, server_id: Long, ts_sec: Long, gtid: Option[String] = None, file: String, pos: Long, row: Int, snapshot: Option[Boolean] = Some(false), thread: Option[Long] = None, db: Option[String] = None, table: Option[String] = None)
