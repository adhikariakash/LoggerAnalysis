import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;

public class Analysis {
    public static void main(String[] args) {

        // Disable logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create a Java Spark Context.
        SparkConf sparkConf = new SparkConf().setAppName("Analysis").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);


        // Load logger data
//        JavaRDD<String> loggers = sc.textFile("/home/nineleaps/IdeaProjects/LoggerAnalysis/src/snippet.txt", 2).toJavaRDD();
        JavaRDD<String> loggers = sc.textFile("/home/nineleaps/IdeaProjects/LoggerAnalysis/src/ghtorrent-logs.txt", 2).toJavaRDD();
        JavaRDD<String> rdd = loggers.map(s -> s.split("\n"))
                .map(l -> Arrays.toString(l).replaceAll(" -- |.rb: |, ghtorrent-", ", "));

        JavaRDD<String[]> loggerRdd = rdd.map(s -> s.split(", "));

        Function2<Integer, Integer, Integer> reduceSumFunc = Integer::sum;

        numberOfLines(loggerRdd);

        numberOfWarnings(loggerRdd);

        numberOfRepositories(loggerRdd);

        mostHttpRequest(loggerRdd);

        mostHttpFailedRequest(loggerRdd,reduceSumFunc);

        mostActiveHour(loggerRdd,reduceSumFunc);

        mostActiveRepo(loggerRdd,reduceSumFunc);

        mostFailedAccessKey(loggerRdd,reduceSumFunc);

    }

    private static void mostFailedAccessKey(JavaRDD<String[]> loggerRdd, Function2<Integer, Integer, Integer> reduceSumFunc) {
        // Which access keys are failing most often
        JavaPairRDD<String, Integer> failedAccessKey = loggerRdd.filter(strings -> {
            if (strings.length >= 8)
                return strings[4].contains("Failed") && strings[7].contains("Access:");
            return false;
        }).map(x -> x[7].split("Access: ")[1].split(",", 2)[0])
                .mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey(reduceSumFunc);
        JavaPairRDD<Integer, String> swapfailedAccessKey = failedAccessKey.mapToPair(Tuple2::swap);
        swapfailedAccessKey = swapfailedAccessKey.sortByKey(false);

        System.out.println("Most failed access key is:");
        for (Tuple2<Integer, String> element : swapfailedAccessKey.take(1)) {
            System.out.println("(" + element._2 + ", " + element._1 + ")");
        }
    }

    private static void mostActiveRepo(JavaRDD<String[]> loggerRdd, Function2<Integer, Integer, Integer> reduceSumFunc) {
        // What is the most active repository
//        loggerRdd.filter(x->x.length>=8).collect().forEach(strings -> System.out.println(strings[4]+"\t"
//                +strings[4].split("/")[4]+"/"+strings[4].split("/")[5]));
        JavaPairRDD<String, Integer> repoNames = loggerRdd.filter(x -> x.length >= 8).map(strings -> {
            try {
                String[] split = strings[4].split("/");
                if (split[5].contains("?")) {
                    String repoName = split[5].split("\\?")[0];
                    return split[4] + "/" + repoName;
                }
                return split[4] + "/" + split[5];
            }catch (ArrayIndexOutOfBoundsException e){
                return null;
            }
        }).mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey(reduceSumFunc);

        JavaPairRDD<Integer, String> swapRepoNames = repoNames.mapToPair(Tuple2::swap);
        swapRepoNames = swapRepoNames.sortByKey(false);
        System.out.println("Most Active repository is:");
        for (Tuple2<Integer, String> element : swapRepoNames.take(1)) {
            System.out.println("(" + element._2 + ", " + element._1 + ")");
        }
    }

    private static void mostActiveHour(JavaRDD<String[]> loggerRdd, Function2<Integer, Integer, Integer> reduceSumFunc) {
        // What is the most active hour of day
        JavaRDD<String> hours = loggerRdd.filter(strings -> {
            if (strings.length>2)
                return true;
            return false;
        }).map(strings -> strings[1].split("T")[1]).map(s -> s.split(":")[0]);
        JavaPairRDD<String, Integer> hoursRdd = hours.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey(reduceSumFunc);
        JavaPairRDD<Integer, String> swapHoursRdd = hoursRdd.mapToPair(Tuple2::swap);
        swapHoursRdd = swapHoursRdd.sortByKey(false);

        System.out.println("Most active hours are:");
        for (Tuple2<Integer, String> element : swapHoursRdd.take(3)) {
            System.out.println("(" + element._2 + ", " + element._1 + ")");
        }
    }

    private static void mostHttpFailedRequest(JavaRDD<String[]> loggerRdd, Function2<Integer, Integer, Integer> reduceSumFunc) {
        // Which client did most FAILED HTTP requests
        JavaPairRDD<String, Integer> failedRequest = loggerRdd.filter(x -> {
            if (x.length>=4)
                return x[3].equals("api_client") && x[4].contains("Failed");
            return false;
        }).mapToPair(e -> new Tuple2<>(e[2], 1)).reduceByKey(reduceSumFunc);
        JavaPairRDD<Integer, String> swapFailedRdd = failedRequest.mapToPair(Tuple2::swap);
        swapFailedRdd = swapFailedRdd.sortByKey(false);

        System.out.println("Client with most failed HTTP request are:");
        for (Tuple2<Integer, String> element : swapFailedRdd.take(3)) {
            System.out.println("(" + element._2 + ", " + element._1 + ")");
        }

    }

    private static void mostHttpRequest(JavaRDD<String[]> loggerRdd) {
        // Which client did most HTTP requests
        Function2<Integer, Integer, Integer> reduceSumFunc = Integer::sum;
        JavaPairRDD<String, Integer> apiClient = loggerRdd.filter(x -> {
            if (x.length>3)
                return x[3].equals("api_client");
            return false;
        }).mapToPair(e -> new Tuple2<>(e[2], 1)).reduceByKey(reduceSumFunc);
//        loggerRdd.collect().forEach(strings -> System.out.println(Arrays.toString(strings)));
        JavaPairRDD<Integer, String> swapRdd = apiClient.mapToPair(Tuple2::swap);
        swapRdd = swapRdd.sortByKey(false);

        System.out.println("Client with most HTTP request are:");
        for (Tuple2<Integer, String> element : swapRdd.take(3)) {
            System.out.println("(" + element._2 + ", " + element._1 + ")");
        }
    }

    private static void numberOfRepositories(JavaRDD<String[]> loggerRdd) {
        //  How many repositories where processed in total? Use the api_client
        JavaRDD<Object> api_client = loggerRdd.flatMap(new FlatMapFunction<String[], Object>() {
            @Override
            public Iterable<Object> call(String[] strings) throws Exception {
                if (strings.length >= 4 && strings[3].contains("api_client")) {
                    return Arrays.asList(strings[4]);
                }
                return Collections.emptyList();
            }
        });

        JavaRDD<Object> javaRDD = api_client.flatMap(new FlatMapFunction<Object, Object>() {
            @Override
            public Iterable<Object> call(Object o) throws Exception {
                String[] split = o.toString().split("/");
                if (split.length == 6) {
                    String t = split[4] + split[5];
                    return Arrays.asList(t.split("\\?")[0]);
                }

                return Collections.emptyList();
            }
        });

        System.out.println(javaRDD.count());
    }

    private static void numberOfWarnings(JavaRDD<String[]> loggerRdd) {
        // Count the number of WARNing messages
        System.out.println("Number of warning messages are: ");
        System.out.println(loggerRdd.filter(k -> k[0].contains("WARN")).count());
    }

    private static void numberOfLines(JavaRDD<String[]> loggerRdd) {
        // How many lines does the RDD contain
        System.out.println("Number of lines are: ");
        System.out.println(loggerRdd.count());
    }
}
