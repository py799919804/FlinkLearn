import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MySink extends RichSinkFunction[String]{

  override def close(): Unit = super.close()


}
