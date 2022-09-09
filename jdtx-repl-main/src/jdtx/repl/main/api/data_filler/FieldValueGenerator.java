package jdtx.repl.main.api.data_filler;

import java.util.*;

public abstract class FieldValueGenerator implements IFieldValueGenerator {

    Random rnd;

    public FieldValueGenerator() {
        this.rnd = new Random();
    }

}
