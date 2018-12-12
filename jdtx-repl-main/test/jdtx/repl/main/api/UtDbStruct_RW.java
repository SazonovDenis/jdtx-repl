package jdtx.repl.main.api;

import jandcode.utils.easyxml.*;
import jdtx.repl.main.api.struct.*;

public class UtDbStruct_RW {

    String fileName;

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void write(IJdxDbStruct struct) throws Exception {
        write(struct, fileName);
    }

    public void write(IJdxDbStruct struct, String fileName) throws Exception {
        EasyXml xml = new EasyXml();

        //
        for (IJdxTableStruct t : struct.getTables()) {
            writeTableStruct(t, xml);
        }

        //
        xml.save().toFile(fileName);
    }

    private void writeTableStruct(IJdxTableStruct table, EasyXml xml) {
        EasyXml item_table = new EasyXml();
        xml.addChild(item_table);

        //
        item_table.setName("table");
        item_table.setValue("@name", table.getName());

        //
        for (IJdxFieldStruct f : table.getFields()) {
            writeFieldStruct(f, item_table);
        }
    }

    private void writeFieldStruct(IJdxFieldStruct field, EasyXml xml) {
        EasyXml item_field = new EasyXml();
        item_field.setName("field");
        item_field.setValue("@name", field.getName());
        item_field.setValue("@size", field.getSize());
        item_field.setValue("@dbdatatype", field.getDbDatatype());
        //
        xml.addChild(item_field);
    }

    public IJdxDbStruct read() throws Exception {
        return read(fileName);
    }

    public IJdxDbStruct read(String fileName) throws Exception {
        EasyXml xml = new EasyXml();
        xml.load().fromFile(fileName);

        //
        IJdxDbStruct struct = new JdxDbStruct();
        //
        for (EasyXml item_table : xml.getChilds()) {
            JdxTableStruct table = new JdxTableStruct();
            struct.getTables().add(table);
            //
            table.setName(item_table.getValueString("@name"));
            //
            readTableStruct(item_table, table);
        }

        //
        return struct;
    }

    private void readTableStruct(EasyXml xml, JdxTableStruct table) {
        for (EasyXml item_field : xml.getChilds()) {
            JdxFieldStruct field = new JdxFieldStruct();
            table.getFields().add(field);
            //
            field.setName(item_field.getValueString("@name"));
            field.setDbDatatype(item_field.getValueString("@dbdatatype"));
            field.setSize(item_field.getValueInt("@size"));
        }
    }

}
