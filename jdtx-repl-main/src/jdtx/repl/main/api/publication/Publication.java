package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;

/**
 * Реализация IPublication
 */
public class Publication implements IPublication {


    JSONArray data;

    public void loadRules(Reader r) throws Exception {
        JSONParser p = new JSONParser();
        data = (JSONArray) p.parse(r);
    }

    public JSONArray getData() {
        return data;
    }

    public void setData(JSONArray data) {
        this.data = data;
    }

    public static String prepareFiledsString(IJdxTableStruct table, String fields) {
        if (fields.compareToIgnoreCase("*") == 0) {
            StringBuilder sb = new StringBuilder();
            // DbUtils.ID_FIELD пусть будет всегда спереди (необязательно, но во-первых это красиво)
            sb.append(DbUtils.ID_FIELD);
            for (IJdxFieldStruct f : table.getFields()) {
                if (f.getName().equalsIgnoreCase(DbUtils.ID_FIELD)) {
                    continue;
                }
                sb.append(",");
                sb.append(f.getName());
            }
            fields = sb.toString();
        } else {
            fields = DbUtils.ID_FIELD + "," + fields;
        }

        return fields.toUpperCase();
    }


}