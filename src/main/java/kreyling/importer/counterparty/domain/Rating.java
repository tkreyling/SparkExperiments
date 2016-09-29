package kreyling.importer.counterparty.domain;

import kreyling.sparkexperiments.Exp2;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;

/**
 * Created by svarpe on 29/09/16.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class Rating extends Exp2.BaseDto{

    @XmlTransient
    private String counterpartyId;

    private String type;

    private String grade;

    private String validFrom;

    private String validTill;

    public static Rating fromCsv(String csvLine) {
        String[] attributes = csvLine.split(";");
        return new Rating(attributes[0], attributes[1], attributes[2],attributes[3],attributes[4]);
    }
}
