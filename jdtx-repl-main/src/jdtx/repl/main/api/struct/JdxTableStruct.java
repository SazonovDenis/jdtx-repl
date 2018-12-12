package jdtx.repl.main.api.struct;

import java.util.*;

public class JdxTableStruct implements IJdxTableStruct {
    String name;
    ArrayList<IJdxFieldStruct> fields = new ArrayList<IJdxFieldStruct>();
    ArrayList<IJdxFieldStruct> primaryKeys = new ArrayList<IJdxFieldStruct>();
    ArrayList<IJdxForeignKey> foreignKeys = new ArrayList<IJdxForeignKey>();


    public String getName() {
        return name;
    }


    public ArrayList<IJdxFieldStruct> getFields() {
        return fields;
    }

    public ArrayList<IJdxFieldStruct> getPrimaryKey() {
        return primaryKeys;
    }

    public ArrayList<IJdxForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    /*
    public IJdxTableStruct cloneTable() {
        JdxTableStruct newTableStruct = new JdxTableStruct();
        newTableStruct.setName(this.getName());
        // ����
        for (IJdxFieldStruct f : this.getFields()) {
            IJdxFieldStruct f1 = f.cloneField();
            // ��������� ���� � �������
            newTableStruct.getFields().add(f1);
        }
        // ��������� �����
        for (IJdxFieldStruct pk : this.getPrimaryKey()) {
            IJdxFieldStruct pk1 = pk.cloneField();
            // ��������� ���� � �������
            newTableStruct.getPrimaryKey().add(pk1);
        }
        // ������� �����
        for (IJdxForeignKey fk : this.getForeignKeys()) {
            IJdxForeignKey fk1 = fk.cloneForeignKey();
            // ��������� ���� � �������
            newTableStruct.getForeignKeys().add(fk1);
        }
        return newTableStruct;
    }
    */

    public void setName(String name) {
        this.name = name;
    }

    public IJdxFieldStruct getField(String fieldName) {
        for (IJdxFieldStruct f : fields) {
            if ((f.getName()).compareToIgnoreCase(fieldName) == 0) {
                return f;
            }
        }
        return null;
    }

}
