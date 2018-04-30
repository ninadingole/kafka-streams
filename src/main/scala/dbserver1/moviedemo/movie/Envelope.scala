package dbserver1.moviedemo.movie

import io.debezium.connector.mysql.Source

case class Envelope(before: Option[Movie] = None,
                    after: Option[Movie] = None,
                    source: Source,
                    op: String,
                    ts_ms: Option[Long] = None)
