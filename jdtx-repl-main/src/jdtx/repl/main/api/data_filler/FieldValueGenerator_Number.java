package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

public class FieldValueGenerator_Number extends FieldValueGenerator {

    double min = 0.0;
    double max = 1e6;
    double precisionScale = 1000;

    public FieldValueGenerator_Number() {
        super();
    }

    public FieldValueGenerator_Number(Double min, Double max, Integer precision) {
        super();
        this.min = min;
        this.max = max;
        this.precisionScale = Math.pow(10, precision);
    }

    @Override
    public Double genValue(IJdxField field) {
        double value = min + rnd.nextDouble() * (max - min);
        value = Math.round(value * precisionScale) / precisionScale;
        return value;
    }

}
