package kreyling.sparkexperiments;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Exp1 {

    public static class Person implements Serializable {
        public final int id;
        public final String forename;
        public final String surname;

        public Person(int id, String forename, String surname) {
            this.id = id;
            this.forename = forename;
            this.surname = surname;
        }

        public static Person fromCsv(String csvLine) {
            String[] attributes = csvLine.split(",");
            return new Person(Integer.parseInt(attributes[0]), attributes[1], attributes[2]);
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Exp1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //List<Integer> data = Arrays.asList(1,2,3,4,5);
        //JavaRDD<Integer> distData = sc.parallelize(data);

        JavaRDD<Person> persons = sc.textFile("src/main/resources/persons.csv").map(Person::fromCsv);
        JavaRDD<String> knowledge = sc.textFile("src/main/resources/knowledge.csv");

        JavaPairRDD<Integer, Person> personsWithId = persons.mapToPair(p -> new Tuple2(p.id, p));
        JavaPairRDD<Integer, String> knowledgeWithId = knowledge.mapToPair(line -> new Tuple2(Integer.parseInt(line.split(",")[0]), line.split(",")[1]));

        persons.collect().forEach(System.out::println);
        knowledge.collect().forEach(System.out::println);

        personsWithId.leftOuterJoin(knowledgeWithId).collect().forEach(System.out::println);
    }
}