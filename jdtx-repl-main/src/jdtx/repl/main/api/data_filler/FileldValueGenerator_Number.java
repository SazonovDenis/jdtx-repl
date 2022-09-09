package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

public class FileldValueGenerator_Number extends FileldValueGenerator {

    double min = 0.0;
    double max = 1e6;
    double precision = 1000;

    public FileldValueGenerator_Number() {
        super();
    }

    public FileldValueGenerator_Number(Double min, Double max, Integer precision) {
        super();
        this.min = min;
        this.max = max;
        this.precision = Math.pow(10, precision);
    }

    @Override
    public Double genValue(IJdxField field) {
        double value = min + rnd.nextDouble() * (max - min);
        value = Math.round(value * precision) / precision;
        return value;
    }

}
