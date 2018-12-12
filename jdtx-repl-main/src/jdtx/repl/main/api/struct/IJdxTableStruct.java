package jdtx.repl.main.api.struct;

import java.util.*;

public interface IJdxTableStruct {

    String getName();

    ArrayList<IJdxFieldStruct> getFields();

    ArrayList<IJdxFieldStruct> getPrimaryKey();

    ArrayList<IJdxForeignKey> getForeignKeys();

    //IJdxTableStruct cloneTable();

    /**
     * ���������� ���� �� �����
     *
     * @param tableName ��� ����
     * @return ���� ��� null, ���� ������ ���� ���
     */
    IJdxFieldStruct getField(String tableName);

}
