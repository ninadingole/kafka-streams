package dbserver1.moviedemo.MOVIE

import io.debezium.connector.mysql.Source

case class Envelope(before: Option[Value] = None, after: Option[Value] = None, source: Source, op: String, ts_ms: Option[Long] = None)