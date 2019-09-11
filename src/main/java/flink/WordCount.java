package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @ Author     ：zsp
 * @ Date       ：Created in 11:07 2019/9/11
 * @ Description：
 * @ Modified By：
 * @ Version:
 */
public class WordCount
{
    public static void main(String[] args)
        throws Exception
    {
        // 定义socket端口号
        int port;
        try
        {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }
        catch (Exception e)
        {
            System.err.println("指定port参数，默认值为9000");
            port = 9000;
        }

        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("DaFa1", 19888);

        // 计算wordcount
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>()
        {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out)
                throws Exception
            {
                String[] splits = value.split(" ");
                for (String word : splits)
                {
                    out.collect(new WordWithCount(word, 1));
                }
            }
        })
            .keyBy("word")
            .timeWindow(Time.seconds(2), Time.seconds(1)) // 定义了一个滑动窗口，窗口大小为2s，每1s滑动一次
            .sum("count");

        // 数据输出到控制台
        windowCount.print().setParallelism(1);

        // flink是懒加载，所以必须调用execute方法
        env.execute("streaming word count");
    }
}
