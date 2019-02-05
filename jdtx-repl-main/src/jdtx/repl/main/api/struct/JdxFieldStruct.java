package jdtx.repl.main.api.struct;

public class JdxFieldStruct implements IJdxFieldStruct {
    String name;
    String type;
    JdxDataType jdxType;
    int size;
    boolean isPrimaryKey;
    IJdxTableStruct refTable;

    public String getName() {
        return name;
    }

    public String getDbDatatype() {
        return type;
    }

    public JdxDataType getJdxDatatype() {
        return jdxType;
    }

    public int getSize() {
        return size;
    }

    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }

    public IJdxTableStruct getRefTable() {
        return this.refTable;
    }

    public IJdxFieldStruct cloneField() {
        JdxFieldStruct f1 = new JdxFieldStruct();
        f1.setName(this.getName());
        f1.setDbDatatype(this.getDbDatatype());
        f1.setJdxDatatype(this.getJdxDatatype());
        f1.setSize(this.getSize());
        f1.setIsPrimaryKey(this.isPrimaryKey);
        f1.setRefTable(this.refTable);
        return f1;
    }

    public void setIsPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public void setRefTable(IJdxTableStruct refTable) {
        this.refTable = refTable;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDbDatatype(String type) {
        this.type = type;
    }

    public void setJdxDatatype(JdxDataType jdxType) {
        this.jdxType = jdxType;
    }

    public void setSize(int size) {
        this.size = size;
    }
}