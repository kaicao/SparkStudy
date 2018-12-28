import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created by kai on 25/12/2018.
 */
public class ShakespeareReadApp {

    public static void main(String[] args) {
        // TODO change to correct path of SparkStudy project's resources folder, where shakespeare.txt located
        String projectPath = "/Users/kai/Documents/Projects/SparkStudy/resources/";
        String shakespeareFile = projectPath + "shakespeare.txt";
        if (!Files.exists(Paths.get(shakespeareFile))) {
            throw new IllegalStateException(String.format("File %s does not exist", shakespeareFile));
        }
        SparkSession spark = SparkSession.builder()
                .appName("Shakespeare reader")
                .master("local[*]") // * means Spark will determine threads to be used by the app's Driver according to the running machine's available cores
                .getOrCreate();

        // 1. Read text
        Dataset<String> data = spark.read().textFile(shakespeareFile);
        // 2. Split lines to words in lower case and avoid empty spaces
        Dataset<Row> words = data.flatMap((String line) ->
                        Arrays.asList(line.split(" ")).iterator(),
                Encoders.STRING())
                .filter((String word) -> word != null && word.trim().length() != 0)
                .map((MapFunction<String, Tuple2<String, Integer>>) word ->
                                new Tuple2<>(word.trim().toLowerCase(), 1),
                        Encoders.tuple(Encoders.STRING(), Encoders.INT()))
                .toDF("word", "count");

        // 3. Calculate total count of each word, also repartitioned to 8 partitions (default 200 which cause more tasks and thus slower),
        // when running in my local machine, it has 8 cores and assigned to this app's Driver by local[*]
        Dataset<Row> wordsWithTotalCounts = words.groupBy(words.col("word"))
                .sum("count")
                .toDF("word", "totalCount")
                .repartition(8, new Column("word"));

        // 4. Cache the words and their total count for any future use, e.g. statistic summary
        wordsWithTotalCounts.cache();

        // 5. Show words with total count (default show top 20 most occurred words)
        wordsWithTotalCounts
                .sort(wordsWithTotalCounts.col("totalCount").desc())
                .show();

        // 6. Show statistics for totalCount values
        wordsWithTotalCounts.select(wordsWithTotalCounts.col("totalCount"))
                .summary()
                .show();

        // Remember to stop the Spark app
        spark.stop();
    }
}
