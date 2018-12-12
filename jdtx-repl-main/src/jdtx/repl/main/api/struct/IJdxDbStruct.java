package jdtx.repl.main.api.struct;

import java.util.*;

public interface IJdxDbStruct {

    public ArrayList<IJdxTableStruct> getTables();

    /**
     * ���������� ������� �� �����
     *
     * @param tableName ��� �������
     * @return ������� ��� null, ���� ����� ������� ���
     */
    public IJdxTableStruct getTable(String tableName);

}
