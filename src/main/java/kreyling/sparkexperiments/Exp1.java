package kreyling.sparkexperiments;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;

public class Exp1 {

    public abstract static class BaseDto implements Serializable {
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    @Value
    @AllArgsConstructor
    public static class KnowledgeItem extends BaseDto {
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
    public static class Person extends BaseDto {
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
        long start = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("Exp1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Person> persons = sc.textFile("src/main/resources/kreyling/sparkexperiments/persons.csv").map(Person::fromCsv);
        JavaRDD<KnowledgeItem> knowledge = sc.textFile("src/main/resources/kreyling/sparkexperiments/knowledge.csv").map(KnowledgeItem::fromCsv);
        JavaRDD<String[]> interests = sc.textFile("src/main/resources/kreyling/sparkexperiments/interests.csv").map(s -> s.split(","));

        JavaPairRDD<Integer, Person> personsWithId = persons.mapToPair(p -> new Tuple2<>(p.id, p));
        JavaPairRDD<Integer, KnowledgeItem> knowledgeWithId = knowledge.mapToPair(k -> new Tuple2<>(k.personId, k));
        JavaPairRDD<Integer, String> interestsWithId = interests.mapToPair(line -> new Tuple2<>(Integer.parseInt(line[0]), line[1]));

        personsWithId = leftOuterJoinList(personsWithId, knowledgeWithId, Person::enrichWithKnowledge);
        personsWithId = leftOuterJoinList(personsWithId, interestsWithId, Person::enrichWithInterests);

        System.out.println(personsWithId.toDebugString());

        personsWithId.collect().forEach(System.out::println);
        long end = System.currentTimeMillis();

        System.out.println(end - start);

        Thread.sleep(10000);
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