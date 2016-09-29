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

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Exp1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> persons = sc.textFile("src/main/resources/kreyling/sparkexperiments/persons.csv");

        JavaPairRDD<String, List<String>> personsByKey = persons
                .map(s -> s.split(","))
                .mapToPair(cols -> new Tuple2<>(cols[0], Arrays.asList(cols)));

        JavaRDD<String> interests = sc.textFile("src/main/resources/kreyling/sparkexperiments/interests.csv");

        JavaPairRDD<String, String> interestsByKey = interests
                .map(s -> s.split(","))
                .mapToPair(cols -> new Tuple2<>(cols[0], cols[1]));


        JavaPairRDD<String, Iterable<String>> groupedInterests = interestsByKey.groupByKey();

        JavaPairRDD<String, Tuple2<List<String>, Optional<Iterable<String>>>> personsWithInterests = personsByKey.leftOuterJoin(groupedInterests);

        personsWithInterests.mapValues(tuple -> {
            tuple._1.add(tuple._2.or(Collections.emptyList()) + "");
            return tuple._1;
        });

        personsByKey.collect().forEach(System.out::println);
        personsWithInterests.collect().forEach(System.out::println);

        System.out.println("Done!");
    }
}