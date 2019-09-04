package me.yanri

import org.apache.flink.streaming.api.scala._

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val text = env.socketTextStream("localhost", 9999, '\n')
        val windowCounts = text.flatMap(x => {
            x.split("\\s")
        }).map(x => {
            WordWithCount(x, 1)
        }).keyBy("word")
            .timeWindow(Time.seconds(5), Time.seconds(1))
            .sum("count")
        windowCounts.print().setParallelism(1)
        env.execute("Socker Word Count")
    }

    case class WordWithCount(word: String, count: Long)
}
