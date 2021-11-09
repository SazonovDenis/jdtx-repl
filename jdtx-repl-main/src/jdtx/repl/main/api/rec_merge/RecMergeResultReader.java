package jdtx.repl.main.api.rec_merge;

import java.io.*;
import java.util.*;

public class RecMergeResultReader {

    // todo Читатели/ писатели Результата -имплементация

    public RecMergeResultReader(InputStream inputStream) {
    }

    public MergeResultTableItem nextResultTable() {
        return new MergeResultTableItem(null, 0);
    }

    public Map<String, Object> nextRec() {
        return null;
    }

    public void close() {

    }

}
