package kreyling.importer.counterparty.domain;

import kreyling.sparkexperiments.Exp2;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by svarpe on 29/09/16.
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Counterparty extends Exp2.BaseDto {

    private String id;

    private String name;

    private String type;

    @XmlElementWrapper(name = "ratings")
    @XmlElement(name = "rating")
    private List<Rating> ratings;


    public static Counterparty fromCsv(String csvLine) {
        String[] attributes = csvLine.split(";");
        return new Counterparty(attributes[0], attributes[1], attributes[2], Collections.emptyList());
    }

    public static Counterparty enrichWithRating(Counterparty counterparty, Iterable<Rating> raitings) {
        List<Rating> rat = new ArrayList<>();
        raitings.forEach(rat::add);
        return new Counterparty(counterparty.getId(), counterparty.getName(), counterparty.getType(), rat);
    }
}
