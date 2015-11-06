package kreyling.sparkexperiments;

import com.google.common.base.Optional;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.BiFunction;

public class Exp1 {

    public static class Person implements Serializable {
        public final int id;
        public final String forename;
        public final String surname;
        public final Iterable<String> knowledge;
        public final Iterable<String> interests;

        public Person(int id, String forename, String surname, Iterable<String> knowledge, Iterable<String> interests) {
            this.id = id;
            this.forename = forename;
            this.surname = surname;
            this.knowledge = knowledge;
            this.interests = interests;
        }

        public static Person enrichWithKnowledge(Person person, Iterable<String> knowledge) {
            return new Person(person.id, person.forename, person.surname, knowledge, person.interests);
        }

        public static Person enrichWithInterests(Person person, Iterable<String> interests) {
            return new Person(person.id, person.forename, person.surname, person.knowledge, interests);
        }

        public static Person fromCsv(String csvLine) {
            String[] attributes = csvLine.split(",");
            return new Person(Integer.parseInt(attributes[0]), attributes[1], attributes[2], Collections.emptyList(), Collections.emptyList());
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("Exp1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Person> persons = sc.textFile("src/main/resources/persons.csv").map(Person::fromCsv);
        JavaRDD<String[]> knowledge = sc.textFile("src/main/resources/knowledge.csv").map(s -> s.split(","));
        JavaRDD<String[]> interests = sc.textFile("src/main/resources/interests.csv").map(s -> s.split(","));

        JavaPairRDD<Integer, Person> personsWithId = persons.mapToPair(p -> new Tuple2<>(p.id, p));
        JavaPairRDD<Integer, String> knowledgeWithId = knowledge.mapToPair(line -> new Tuple2<>(Integer.parseInt(line[0]), line[1]));
        JavaPairRDD<Integer, String> interestsWithId = interests.mapToPair(line -> new Tuple2<>(Integer.parseInt(line[0]), line[1]));

        personsWithId = leftOuterJoinList(personsWithId, knowledgeWithId, Person::enrichWithKnowledge);
        personsWithId = leftOuterJoinList(personsWithId, interestsWithId, Person::enrichWithInterests);

        System.out.println(personsWithId.toDebugString());

        personsWithId.collect().forEach(System.out::println);
        long end = System.currentTimeMillis();

        System.out.println(end - start);
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