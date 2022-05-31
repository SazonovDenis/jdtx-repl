package jdtx.repl.main.ut;

import jandcode.utils.*;
import org.joda.time.*;

import java.util.*;

public class RandomString {

    Random rnd;

    public RandomString() {
        rnd = new Random(new DateTime().getMillis());
    }

    public RandomString(long seed) {
        rnd = new Random(seed);
    }

    public String nextHexStr(int len) {
        byte[] x = new byte[(len / 2) + 1];
        rnd.nextBytes(x);
        String s = UtString.toHexString(x);
        s = s.substring(0, len);
        return s;
    }

}
