package kreyling.importer.counterparty;

import kreyling.importer.counterparty.domain.Counterparty;
import kreyling.importer.counterparty.domain.Rating;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.util.Collections;


/**
 * Created by svarpe on 29/09/16.
 */
public class CounterPartyImporter {

    public static final String RESOURCE_PATH = "/Applications/mesos-1.0.1/share/";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(CounterPartyImporter.class.getSimpleName())
                .set("spark.hadoop.validateOutputSpecs", "false") // Overwrite output files
                .set("spark.executor.uri", "http://10.89.0.96:8888/spark-2.0.0-bin-hadoop2.7.tgz"); //Download spark binaries from here

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Counterparty> counterParty = sc.textFile(RESOURCE_PATH +
                "counter-party.csv").map(Counterparty::fromCsv);
        JavaRDD<Rating> rating = sc.textFile(RESOURCE_PATH +
                "rating.csv").map(Rating::fromCsv);

        JavaPairRDD<String, Counterparty> counterpartyWithId = counterParty.repartition(3).mapToPair(cp -> new Tuple2<>(cp.getId(), cp));
        JavaPairRDD<String, Iterable<Rating>> groupedratings = rating.groupBy(r -> r.getCounterpartyId());

        JavaPairRDD<String, Tuple2<Counterparty, org.apache.spark.api.java.Optional<Iterable<Rating>>>> cpCpmbinedWithRatings
                = counterpartyWithId.
                leftOuterJoin(groupedratings);

        JavaPairRDD<String, Counterparty> counterPartyWithRating = cpCpmbinedWithRatings
                .mapValues(tuple -> Counterparty.enrichWithRating(tuple._1, tuple._2.or(Collections.EMPTY_LIST)));

        counterPartyWithRating.map(t -> marshallToString(t._2)).saveAsTextFile(RESOURCE_PATH + "CIS.txt");
        // .collect().forEach(System.out::println);
        sc.stop();
    }

    private static String marshallToString(Counterparty counterparty) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(Counterparty.class, Rating.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(counterparty, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }
}
