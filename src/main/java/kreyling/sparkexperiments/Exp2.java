package kreyling.sparkexperiments;

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

public class Exp2 {

    public static final String RESOURCE_PATH = "./";

    public abstract static class BaseDto implements Serializable {
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    public static class KnowledgeItem extends BaseDto {
        public final int personId;
        public final String name;
        public final String level;

        public KnowledgeItem(int personId, String name, String level) {
            this.personId = personId;
            this.name = name;
            this.level = level;
        }

        public static KnowledgeItem fromCsv(String csvLine) {
            String[] attributes = csvLine.split(",");
            return new KnowledgeItem(Integer.parseInt(attributes[0]), attributes[1], attributes[2]);
        }
    }

    public static class Person extends BaseDto {
        public final int id;
        public final String forename;
        public final String surname;
        public final Iterable<KnowledgeItem> knowledge;
        public final Iterable<String> interests;

        public Person(int id, String forename, String surname, Iterable<KnowledgeItem> knowledge, Iterable<String> interests) {
            this.id = id;
            this.forename = forename;
            this.surname = surname;
            this.knowledge = knowledge;
            this.interests = interests;
        }

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

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SparkConf conf = new SparkConf()
                .setAppName(Exp2.class.getSimpleName())
                .set("spark.hadoop.validateOutputSpecs", "false") // Overwrite output files
                .set("spark.executor.uri", "http://10.89.0.96:8888/spark-2.0.0-bin-hadoop2.7.tgz"); //Download spark binaries from here

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Person> persons = sc.textFile(RESOURCE_PATH +
                "persons.csv").map(Person::fromCsv);
        JavaRDD<KnowledgeItem> knowledge = sc.textFile(RESOURCE_PATH +
                "knowledge.csv").map(KnowledgeItem::fromCsv);
        JavaRDD<String[]> interests = sc.textFile(RESOURCE_PATH +
                "interests.csv").map(s -> s.split(","));

        JavaPairRDD<Integer, Person> personsWithId = persons.mapToPair(p -> new Tuple2<>(p.id, p));
        JavaPairRDD<Integer, KnowledgeItem> knowledgeWithId = knowledge.mapToPair(k -> new Tuple2<>(k.personId, k));
        JavaPairRDD<Integer, String> interestsWithId = interests.mapToPair(line -> new Tuple2<>(Integer.parseInt(line[0]), line[1]));

        personsWithId = personsWithId
                .leftOuterJoin(knowledgeWithId.groupByKey())
                .mapValues(tuple1 -> Person.enrichWithKnowledge(tuple1._1(), tuple1._2().or(Collections.emptyList())));
        personsWithId = personsWithId
                .leftOuterJoin(interestsWithId.groupByKey())
                .mapValues(tuple -> Person.enrichWithInterests(tuple._1(), tuple._2().or(Collections.emptyList())));

        System.out.println(personsWithId.rdd().getCreationSite());
        System.out.println(personsWithId.toDebugString());

        personsWithId.collect().forEach(System.out::println);
        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }

}