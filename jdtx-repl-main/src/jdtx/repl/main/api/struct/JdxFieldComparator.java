package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxFieldComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        IJdxField field1 = (IJdxField) o1;
        IJdxField field2 = (IJdxField) o2;
        return field1.getName().compareToIgnoreCase(field2.getName());
    }

}
