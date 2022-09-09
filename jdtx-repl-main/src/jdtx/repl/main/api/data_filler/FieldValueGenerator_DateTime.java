package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;
import org.joda.time.*;

public class FieldValueGenerator_DateTime extends FieldValueGenerator {

    public FieldValueGenerator_DateTime() {
        super();
    }

    @Override
    public DateTime genValue(IJdxField field) {
        DateTime dtBeg = new DateTime("1970-01-01");
        return dtBeg.plusSeconds(rnd.nextInt(50 * 365 * 24 * 60 * 60));
    }

}
