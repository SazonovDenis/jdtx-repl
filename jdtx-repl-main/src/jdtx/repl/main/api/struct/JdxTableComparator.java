package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxTableComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        IJdxTable table1 = (IJdxTable) o1;
        IJdxTable table2 = (IJdxTable) o2;
        return table1.getName().compareToIgnoreCase(table2.getName());
    }

}
