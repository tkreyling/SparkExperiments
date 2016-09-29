package kreyling.importer.counterparty;

import com.google.common.base.Optional;
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

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("CounterpartImporter").setMaster("local");
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Counterparty> counterParty = sc.textFile("src/main/resources/counterparty/counter-party.csv").map(Counterparty::fromCsv);
        JavaRDD<Rating> rating = sc.textFile("src/main/resources/counterparty/rating.csv").map(Rating::fromCsv);

        JavaPairRDD<String, Counterparty> counterpartyWithId = counterParty.mapToPair(cp -> new Tuple2<>(cp.getId(), cp));
        JavaPairRDD<String, Iterable<Rating>> groupedratings = rating.groupBy(r -> r.getCounterpartyId());

        JavaPairRDD<String, Tuple2<Counterparty, Optional<Iterable<Rating>>>> cpCpmbinedWithRatings = counterpartyWithId.
                leftOuterJoin(groupedratings);

        JavaPairRDD<String, Counterparty> counterPartyWithRating = cpCpmbinedWithRatings
                .mapValues(tuple -> Counterparty.enrichWithRating(tuple._1, tuple._2.or(Collections.EMPTY_LIST)));

        counterPartyWithRating.map(t -> marshallToString(t._2)).saveAsTextFile("src/main/resources/counterparty/CIS.txt");
        // .collect().forEach(System.out::println);
        sc.stop();
    }

    private static String marshallToString(Counterparty counterparty) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(Counterparty.class,Rating.class);
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
