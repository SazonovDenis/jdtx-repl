package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxForeignKeyComparator implements Comparator<IJdxForeignKey> {

    @Override
    public int compare(IJdxForeignKey foreignKey1, IJdxForeignKey foreignKey2) {
        return foreignKey1.getName().compareToIgnoreCase(foreignKey2.getName());
    }

}
