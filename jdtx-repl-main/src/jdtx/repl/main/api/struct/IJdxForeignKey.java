package jdtx.repl.main.api.struct;

public interface IJdxForeignKey {

    /**
     * @return ����, ������� ��������� (���������� ��������� ����)
     */
    IJdxFieldStruct getField();

    /**
     * @return �������, �� ������� ���������
     */
    IJdxTableStruct getTable();

    /**
     * @return ���� � �������, �� ������� ���������
     */
    IJdxFieldStruct getTableField();

    void setField(IJdxFieldStruct field);

    void setTable(IJdxTableStruct table);

    void setTableField(IJdxFieldStruct field);

    IJdxForeignKey cloneForeignKey();

}