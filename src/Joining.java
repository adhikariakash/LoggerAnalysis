import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Joining {
    public static void main(String[] args) {

        // Disable logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create a Java Spark Context.
        SparkConf sparkConf = new SparkConf().setAppName("Joining").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);

        // Load interesting input data.
        JavaRDD<String> interesting = sc.textFile("/home/nineleaps/IdeaProjects/LoggerAnalysis/src/important-repos.csv", 2)
                .toJavaRDD();

        // Load logger data
        JavaRDD<String> loggers = sc.textFile("/home/nineleaps/IdeaProjects/LoggerAnalysis/src/ghtorrent-logs.txt", 2).toJavaRDD();
        JavaRDD<String> logger = loggers.map(s -> s.split("\n"))
                .map(l -> Arrays.toString(l).replaceAll(" -- |.rb: |, ghtorrent-", ", "));

        String first = interesting.first();
        interesting = interesting.filter(row -> !row.equals(first));
        JavaRDD<String[]> rdd = interesting.map(line -> line.split(","));

        // How many records are there in the table interesting
        System.out.println("Total number of record is " + rdd.count());

        compareEntries(rdd, logger);

    }

    private static void compareEntries(JavaRDD<String[]> rdd, JavaRDD<String> logger) {
        // How many records in the logger table refer to entries in the interesting table
        JavaRDD<Object> interestingRepo = rdd.map(x -> x[3]);//extracting the repo name from interesting by key
        JavaPairRDD<Object, Object> interestingRepoRdd = interestingRepo.keyBy(x -> x);

        JavaRDD<String[]> javaRDD = logger.map(s -> s.split(","));
        javaRDD = javaRDD.filter(strings -> strings.length >= 5);
        JavaRDD<Object> flatMap = javaRDD.flatMap(new FlatMapFunction<String[], Object>() {
            @Override
            public Iterable<Object> call(String[] strings) throws Exception {
                try {
                    if (strings[4].contains("https") && strings[4].contains("Successful") || strings[4].contains("Failed")) {
                        String[] t = strings[4].split("/");
                        return Arrays.asList(t[5]);
                    }
                    return Collections.emptyList();
                } catch (Exception e) {
                    return Collections.emptyList();
                }
            }
        });

        JavaPairRDD<Object, Object> loggerMap = flatMap.keyBy(k -> k);

        JavaPairRDD<Object, Tuple2<Object, Object>> joinedRepo = interestingRepoRdd.join(loggerMap);
        System.out.println("Entries in the interesting: " + joinedRepo.count());

        // Which of the interesting repositories has the most failed API calls
        mostFailedApiCalls(javaRDD, interestingRepoRdd);
    }

    private static void mostFailedApiCalls(JavaRDD<String[]> javaRDD, JavaPairRDD<Object, Object> interestingRepoRdd) {
        // Which of the interesting repositories has the most failed API calls
        JavaRDD<Object> failedRepo = javaRDD.flatMap(new FlatMapFunction<String[], Object>() {
            @Override
            public Iterable<Object> call(String[] strings) throws Exception {
                try {
                    if (strings[4].contains("https") && strings[4].startsWith(" Failed")) {
                        String[] t = strings[4].split("/");
                        return Arrays.asList(t[5]);
                    }
                    return Collections.emptyList();
                } catch (Exception e) {
                    return Collections.emptyList();
                }
            }
        });

        JavaPairRDD<Object, Object> failedMap = failedRepo.keyBy(k -> k);

        JavaPairRDD<Object, Tuple2<Object, Object>> joinFailedRepo = interestingRepoRdd.join(failedMap);


        JavaRDD<Object> failedRepoPair = joinFailedRepo.flatMap(new FlatMapFunction<Tuple2<Object, Tuple2<Object, Object>>, Object>() {
            @Override
            public Iterable<Object> call(Tuple2<Object, Tuple2<Object, Object>> objectTuple2Tuple2) throws Exception {
                return Arrays.asList(objectTuple2Tuple2._1());
            }
        });

        JavaPairRDD<Object, Integer> repoPair = failedRepoPair.mapToPair(e -> new Tuple2<>(e, 1));

        //Reduce Function for sum
        Function2<Integer, Integer, Integer> reduceSumFunc = Integer::sum;
        JavaPairRDD<Object, Integer> sumRepoPair = repoPair.reduceByKey(reduceSumFunc);

        JavaPairRDD<Integer, Object> swapSumRepoPair = sumRepoPair.mapToPair(Tuple2::swap); // swap the key value pair for sorting
        swapSumRepoPair = swapSumRepoPair.sortByKey(false);

        System.out.println("Top 3 failed request are:");
        for (Tuple2<Integer, Object> element : swapSumRepoPair.take(3)) {
            System.out.println("(" + element._1 + ", " + element._2 + ")");
        }
    }
}
