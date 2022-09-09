package jdtx.repl.main.api.data_filler;

import java.util.*;

public abstract class FileldValueGenerator implements IFileldValueGenerator {

    Random rnd;

    public FileldValueGenerator() {
        this.rnd = new Random();
    }

}
