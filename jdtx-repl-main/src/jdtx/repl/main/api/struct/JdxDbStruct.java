package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxDbStruct implements IJdxDbStruct {

    private ArrayList<IJdxTableStruct> tables;

    public JdxDbStruct() {
        tables = new ArrayList<IJdxTableStruct>();
    }

/*
    public JdxDbStruct cloneStruct() {
        JdxDbStruct newStruct = new JdxDbStruct();
        for (IJdxTableStruct t : this.getTables()) {
            newStruct.getTables().add(t.cloneTable());
        }
        // ремонтируем ссылки в ForeignKey
        for (IJdxTableStruct newTable : newStruct.getTables()) {
            for (IJdxForeignKey fk : newTable.getForeignKeys()) {
                IJdxFieldStruct ownField = fk.getField();
                IJdxTableStruct refTable = fk.getTable();
                IJdxFieldStruct refField = fk.getTableField();
                //
                IJdxFieldStruct newOwnField = newTable.getField(ownField.getName());
                IJdxTableStruct newRefTable = newStruct.getTable(refTable.getName());
                IJdxFieldStruct newRefTableField = newRefTable.getField(refField.getName());
                //
                fk.setField(newOwnField);
                fk.setTable(newRefTable);
                fk.setTableField(newRefTableField);
            }
        }
        //
        return newStruct;
    }
*/

    public ArrayList<IJdxTableStruct> getTables() {
        return tables;
    }

    public IJdxTableStruct getTable(String tableName) {
        for (IJdxTableStruct t : tables) {
            if (t.getName().compareToIgnoreCase(tableName) == 0) {
                return t;
            }
        }
        return null;
    }

}
