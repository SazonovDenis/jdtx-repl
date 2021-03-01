package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxTableComparator implements Comparator<IJdxTable> {

    @Override
    public int compare(IJdxTable table1, IJdxTable table2) {
        return table1.getName().compareToIgnoreCase(table2.getName());
    }

}
