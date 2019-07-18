package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxTable implements IJdxTable {
    String name;
    ArrayList<IJdxField> fields = new ArrayList<IJdxField>();
    ArrayList<IJdxField> primaryKeys = new ArrayList<IJdxField>();
    ArrayList<IJdxForeignKey> foreignKeys = new ArrayList<IJdxForeignKey>();


    public String getName() {
        return name;
    }


    public ArrayList<IJdxField> getFields() {
        return fields;
    }

    public ArrayList<IJdxField> getPrimaryKey() {
        return primaryKeys;
    }

    public ArrayList<IJdxForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    /*
    public IJdxTable cloneTable() {
        JdxTable newTableStruct = new JdxTable();
        newTableStruct.setName(this.getName());
        // поля
        for (IJdxField f : this.getFields()) {
            IJdxField f1 = f.cloneField();
            // добавляем поле в таблицу
            newTableStruct.getFields().add(f1);
        }
        // первичные ключи
        for (IJdxField pk : this.getPrimaryKey()) {
            IJdxField pk1 = pk.cloneField();
            // добавляем ключ в таблицу
            newTableStruct.getPrimaryKey().add(pk1);
        }
        // внешние ключи
        for (IJdxForeignKey fk : this.getForeignKeys()) {
            IJdxForeignKey fk1 = fk.cloneForeignKey();
            // добавляем ключ в таблицу
            newTableStruct.getForeignKeys().add(fk1);
        }
        return newTableStruct;
    }
    */

    public void setName(String name) {
        this.name = name;
    }

    public IJdxField getField(String fieldName) {
        for (IJdxField f : fields) {
            if ((f.getName()).compareToIgnoreCase(fieldName) == 0) {
                return f;
            }
        }
        return null;
    }

}
