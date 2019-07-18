package jdtx.repl.main.api.struct;

import jandcode.utils.easyxml.*;

import java.io.*;
import java.util.*;

/**
 * Читать/писать IJdxDbStruct в XML
 */
public class JdxDbStruct_XmlRW {

    boolean doSort = true;

    public String toString(IJdxDbStruct struct) throws Exception {
        EasyXml xml = new EasyXml();

        //
        for (IJdxTableStruct t : struct.getTables()) {
            writeTableStruct(t, xml);
        }

        //
        return xml.save().toString();
    }

    public void saveToFile(IJdxDbStruct struct, String fileName) throws Exception {
        EasyXml xml = new EasyXml();

        //
        for (IJdxTableStruct t : struct.getTables()) {
            writeTableStruct(t, xml);
        }

        //
        xml.save().toFile(fileName);
    }

    public byte[] getBytes(IJdxDbStruct struct) throws Exception {
        EasyXml xml = new EasyXml();

        //
        for (IJdxTableStruct t : struct.getTables()) {
            writeTableStruct(t, xml);
        }

        //
        return xml.save().toBytes();
    }

    private void writeTableStruct(IJdxTableStruct table, EasyXml xml) {
        EasyXml item_table = new EasyXml();
        xml.addChild(item_table);

        //
        item_table.setName("table");
        item_table.setValue("@name", table.getName());

        //
        List<IJdxFieldStruct> fields;
        List<IJdxForeignKey> foreignKeys;
        if (doSort) {
            fields = new ArrayList<>();
            fields.addAll(table.getFields());
            Collections.sort(fields, new JdxFieldComparator());
            //
            foreignKeys = new ArrayList<>();
            foreignKeys.addAll(table.getForeignKeys());
            Collections.sort(foreignKeys, new JdxForeignKeyComparator());
        } else {
            fields = table.getFields();
            foreignKeys = table.getForeignKeys();
        }

        //
        for (IJdxFieldStruct f : fields) {
            writeFieldStruct(f, item_table);
        }

        //
        for (IJdxForeignKey fk : foreignKeys) {
            writeFkStruct(fk, item_table);
        }
    }

    private void writeFkStruct(IJdxForeignKey fk, EasyXml item_table) {
        EasyXml item_fk = new EasyXml();
        item_fk.setName("fk");
        item_fk.setValue("@name", fk.getName());
        item_fk.setValue("@field", fk.getField().getName());
        item_fk.setValue("@table", fk.getTable().getName());
        item_fk.setValue("@tablefield", fk.getTableField().getName());
        item_table.addChild(item_fk);
    }

    private void writeFieldStruct(IJdxFieldStruct field, EasyXml xml) {
        EasyXml item_field = new EasyXml();
        //
        item_field.setName("field");
        item_field.setValue("@name", field.getName());
        item_field.setValue("@size", field.getSize());
        item_field.setValue("@dbdatatype", field.getDbDatatype());
        //
        xml.addChild(item_field);
    }

    public IJdxDbStruct read(String fileName) throws Exception {
        EasyXml xml = new EasyXml();
        xml.load().fromFile(fileName);
        return read(xml);
    }

    public IJdxDbStruct read(InputStream stream) throws Exception {
        EasyXml xml = new EasyXml();
        xml.load().fromStream(stream);
        return read(xml);
    }

    public IJdxDbStruct read(byte[] bytes) throws Exception {
        EasyXml xml = new EasyXml();
        xml.load().fromBytes(bytes);
        return read(xml);
    }

    public IJdxDbStruct read(EasyXml xml) throws Exception {
        IJdxDbStruct struct = new JdxDbStruct();

        //
        Iterable<EasyXml> childs = xml.getChilds();
        if (childs != null) {
            // Таблицы
            for (EasyXml item_table : childs) {
                JdxTableStruct table = new JdxTableStruct();
                struct.getTables().add(table);
                //
                table.setName(item_table.getValueString("@name"));
                //
                readTableStruct(item_table, table);
            }

            // FK таблиц
            for (EasyXml item_table : childs) {
                IJdxTableStruct table = struct.getTable(item_table.getValueString("@name"));
                //
                readTableFkStruct(item_table, table, struct);
            }
        }

        //
        return struct;
    }

    private void readTableStruct(EasyXml xml, IJdxTableStruct table) {
        for (EasyXml item_xml : xml.getChilds()) {
            if (item_xml.getName().compareToIgnoreCase("field") == 0) {
                JdxFieldStruct field = new JdxFieldStruct();
                table.getFields().add(field);
                //
                field.setName(item_xml.getValueString("@name"));
                field.setDbDatatype(item_xml.getValueString("@dbdatatype"));
                field.setSize(item_xml.getValueInt("@size"));
            }
        }
    }

    private void readTableFkStruct(EasyXml xml, IJdxTableStruct table, IJdxDbStruct struct) {
        for (EasyXml item_xml : xml.getChilds()) {
            if (item_xml.getName().compareToIgnoreCase("fk") == 0) {
                JdxForeignKey fk = new JdxForeignKey();
                table.getForeignKeys().add(fk);
                //
                IJdxTableStruct refTable = struct.getTable(item_xml.getValueString("@table"));
                IJdxFieldStruct refTableField = refTable.getField(item_xml.getValueString("@tablefield"));
                fk.setName(item_xml.getValueString("@name"));
                fk.setField(table.getField(item_xml.getValueString("@field")));
                fk.setTable(refTable);
                fk.setTableField(refTableField);
            }
        }
    }

}
