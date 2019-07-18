package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxFieldComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        IJdxFieldStruct field1 = (IJdxFieldStruct) o1;
        IJdxFieldStruct field2 = (IJdxFieldStruct) o2;
        return field1.getName().compareToIgnoreCase(field2.getName());
    }

}
