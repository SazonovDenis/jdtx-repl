package jdtx.repl.main.api.struct;

public interface IJdxFieldStruct {

    String getName();

    String getDbDatatype();

    JdxDataType getJdxDatatype();

    int getSize();

    boolean isPrimaryKey();

    IJdxFieldStruct cloneField();

}
