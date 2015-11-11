package kreyling.sparkexperiments;

import com.google.common.base.Optional;
import org.apache.commons.lang.builder.ToStringBuilder;

class Apfel {
    public final int id;

    Apfel(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}


public class OptionalExp {

    public static void main(String[] args) {
        Optional<Apfel> keinText = Optional.absent();
        Optional<Apfel> einText = Optional.of(new Apfel(1));

        System.out.println(keinText.or(new Apfel(0)));
        System.out.println(einText.or(new Apfel(0)));
    }
}
