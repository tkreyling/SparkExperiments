package kreyling.sparkexperiments;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.Dependency;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Collections;

public class Exp1 {

    public static final String RESOURCE_PATH = "/Applications/mesos-1.0.1/share/";

    @Value
    @AllArgsConstructor
    public static class KnowledgeItem implements Serializable {
        int personId;
        String name;
        String level;

        public static KnowledgeItem fromCsv(String csvLine) {
            String[] attributes = csvLine.split(",");
            return new KnowledgeItem(Integer.parseInt(attributes[0]), attributes[1], attributes[2]);
        }
    }

    @Value
    @AllArgsConstructor
    public static class Person implements Serializable {
        public final int id;
        public final String forename;
        public final String surname;
        public final Iterable<KnowledgeItem> knowledge;
        public final Iterable<String> interests;

        public static Person enrichWithKnowledge(Person person, Iterable<KnowledgeItem> knowledge) {
            return new Person(person.id, person.forename, person.surname, knowledge, person.interests);
        }

        public static Person enrichWithInterests(Person person, Iterable<String> interests) {
            return new Person(person.id, person.forename, person.surname, person.knowledge, interests);
        }

        public static Person fromCsv(String csvLine) {
            String[] attributes = csvLine.split(",");
            return new Person(Integer.parseInt(attributes[0]), attributes[1], attributes[2], Collections.emptyList(), Collections.emptyList());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (System.getProperty("os.name").contains("Windows")) {
            System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        }

        long start = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setAppName(Exp1.class.getSimpleName())
                .set("spark.hadoop.validateOutputSpecs", "false") // Overwrite output files
                .set("spark.executor.uri", "http://10.89.0.96:8888/spark-2.0.0-bin-hadoop2.7.tgz"); //Download spark binaries from here

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Person> persons = sc.textFile(RESOURCE_PATH + "persons.csv").map(Person::fromCsv);
        JavaRDD<KnowledgeItem> knowledge = sc.textFile(RESOURCE_PATH + "knowledge.csv").map(KnowledgeItem::fromCsv);
        JavaRDD<String[]> interests = sc.textFile(RESOURCE_PATH + "interests.csv").map(s -> s.split(","));

        JavaPairRDD<Integer, Person> personsWithId = persons.mapToPair(p -> new Tuple2<>(p.id, p));
        JavaPairRDD<Integer, KnowledgeItem> knowledgeWithId = knowledge.mapToPair(k -> new Tuple2<>(k.personId, k));
        JavaPairRDD<Integer, String> interestsWithId = interests.mapToPair(line -> new Tuple2<>(Integer.parseInt(line[0]), line[1]));

        personsWithId = leftOuterJoinList(personsWithId, knowledgeWithId, Person::enrichWithKnowledge);
        personsWithId = leftOuterJoinList(personsWithId, interestsWithId, Person::enrichWithInterests);

        RDD<Tuple2<Integer, Person>> rdd = personsWithId.rdd();
        printRdd(rdd, "");

        personsWithId.map(t -> t._2.toString()).saveAsTextFile(RESOURCE_PATH + "persons.txt");
        personsWithId.collect().forEach(System.out::println);
        long end = System.currentTimeMillis();

        System.out.println(end - start);

        Thread.sleep(10000);
    }

    private static void printRdd(RDD<?> rdd, String indent) {
        System.out.println(indent + " *-" + rdd);

        Seq<Dependency<?>> dependencies = rdd.getDependencies();
        Iterator<Dependency<?>> iterator = dependencies.iterator();
        while (iterator.hasNext()) {
            Dependency<?> dependency = iterator.next();
            if (iterator.hasNext()) {
                printRdd(dependency.rdd(), indent + " | ");
            } else {
                printRdd(dependency.rdd(), indent + "   ");
            }
        }
    }

    private static <K, L, R> JavaPairRDD<K, L> leftOuterJoinList(
            JavaPairRDD<K, L> leftItems,
            JavaPairRDD<K, R> rightItems,
            Function2<L, Iterable<R>, L> copyConstructor) {
        return leftItems
                .leftOuterJoin(rightItems.groupByKey())
                .mapValues(tuple -> copyConstructor.call(tuple._1(), tuple._2().or(Collections.emptyList())));
    }
}