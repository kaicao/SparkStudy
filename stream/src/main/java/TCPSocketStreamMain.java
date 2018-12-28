import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by kai on 28/12/2018.
 */
public class TCPSocketStreamMain {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9999;

        SparkConf conf = new SparkConf()
                .setAppName("TCP stream reader")
                .setMaster("local[2]");

        // Establish streaming context with batch interval of 10s
        JavaStreamingContext streamContext = new JavaStreamingContext(
                conf,
                Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = streamContext.socketTextStream(host, port);
        JavaPairDStream<String, Integer> wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((p1, p2) -> p1 + p2);

        wordCounts.print();

        System.out.println("Start streaming context listening tcp/localhost:9999");
        streamContext.start();

        try {
            streamContext.awaitTermination();
            System.out.println("Stopped streaming context listening tcp/localhost:9999");
        } catch (InterruptedException e) {
            System.err.println(String.format("Error when listening on tcp/localhost:9999: %s", e.getMessage()));
            e.printStackTrace();
            streamContext.stop();
        }
    }
}
