package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxDbStruct implements IJdxDbStruct {

    protected List<IJdxTable> tables;

    public JdxDbStruct() {
        tables = new ArrayList<>();
    }

    public List<IJdxTable> getTables() {
        return tables;
    }

    public IJdxTable getTable(String tableName) {
        for (IJdxTable t : tables) {
            if (t.getName().compareToIgnoreCase(tableName) == 0) {
                return t;
            }
        }
        return null;
    }

}
