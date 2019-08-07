package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxDbStruct implements IJdxDbStruct {

    protected List<IJdxTable> tables;

    public JdxDbStruct() {
        tables = new ArrayList<IJdxTable>();
    }

/*
    public JdxDbStruct cloneStruct() {
        JdxDbStruct newStruct = new JdxDbStruct();
        for (IJdxTable t : this.getTables()) {
            newStruct.getTables().add(t.cloneTable());
        }
        // ремонтируем ссылки в ForeignKey
        for (IJdxTable newTable : newStruct.getTables()) {
            for (IJdxForeignKey fk : newTable.getForeignKeys()) {
                IJdxField ownField = fk.getField();
                IJdxTable refTable = fk.getTable();
                IJdxField refField = fk.getTableField();
                //
                IJdxField newOwnField = newTable.getField(ownField.getName());
                IJdxTable newRefTable = newStruct.getTable(refTable.getName());
                IJdxField newRefTableField = newRefTable.getField(refField.getName());
                //
                fk.setField(newOwnField);
                fk.startTable(newRefTable);
                fk.setTableField(newRefTableField);
            }
        }
        //
        return newStruct;
    }
*/

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
