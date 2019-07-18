package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxForeignKeyComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        IJdxForeignKey foreignKey1 = (IJdxForeignKey) o1;
        IJdxForeignKey foreignKey2 = (IJdxForeignKey) o2;
        return foreignKey1.getName().compareToIgnoreCase(foreignKey2.getName());
    }

}
