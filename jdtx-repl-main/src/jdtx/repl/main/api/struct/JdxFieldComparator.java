package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxFieldComparator implements Comparator<IJdxField> {

    @Override
    public int compare(IJdxField field1, IJdxField field2) {
        if (field1.isPrimaryKey() && !field2.isPrimaryKey()) {
            return -1;
        } else if (!field1.isPrimaryKey() && field2.isPrimaryKey()) {
            return 1;
        } else {
            return field1.getName().compareToIgnoreCase(field2.getName());
        }
    }

}
