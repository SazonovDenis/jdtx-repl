package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

public class FileldValueGenerator_Blob extends FileldValueGenerator {

    int maxSize = 1024;

    public FileldValueGenerator_Blob() {
        super();
    }

    public FileldValueGenerator_Blob(int maxSize) {
        super();
        this.maxSize = maxSize;
    }

    @Override
    public byte[] genValue(IJdxField field) {
        int len = rnd.nextInt(maxSize);

        byte[] res = new byte[len];
        rnd.nextBytes(res);

        return res;
    }

}
