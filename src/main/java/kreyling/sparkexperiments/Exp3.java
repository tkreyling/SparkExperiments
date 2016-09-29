package kreyling.sparkexperiments;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Exp3 {

    public static final String RESOURCE_PATH = "/Applications/mesos-1.0.1/share/";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Exp3.class.getSimpleName())
                .set("spark.hadoop.validateOutputSpecs", "false") // Overwrite output files
                .set("spark.executor.uri", "http://10.89.0.96:8888/spark-2.0.0-bin-hadoop2.7.tgz"); //Download spark binaries from here

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> persons = sc.textFile(RESOURCE_PATH + "persons.csv");

        JavaPairRDD<String, List<String>> personsByKey = persons
                .map(s -> s.split(","))
                .mapToPair(cols -> new Tuple2<>(cols[0], Arrays.asList(cols)));

        JavaRDD<String> interests = sc.textFile(RESOURCE_PATH + "interests.csv");

        JavaPairRDD<String, String> interestsByKey = interests
                .map(s -> s.split(","))
                .mapToPair(cols -> new Tuple2<>(cols[0], cols[1]));


        JavaPairRDD<String, Iterable<String>> groupedInterests = interestsByKey.groupByKey();

        JavaPairRDD<String, Tuple2<List<String>, org.apache.spark.api.java.Optional<Iterable<String>>>> personsWithInterests
                = personsByKey.leftOuterJoin(groupedInterests);

        personsWithInterests.mapValues(tuple -> {
            tuple._1.add(tuple._2.or(Collections.emptyList()) + "");
            return tuple._1;
        });

        personsByKey.collect().forEach(System.out::println);
        personsWithInterests.collect().forEach(System.out::println);

        System.out.println("Done!");
    }
}
