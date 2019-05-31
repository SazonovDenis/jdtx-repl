package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

public class Publication implements IPublication {


    IJdxDbStruct publicationStruct = new JdxDbStruct();

    public void loadRules(Reader r, IJdxDbStruct baseStruct) throws Exception {
        JSONParser p = new JSONParser();
        JSONArray publicationData = (JSONArray) p.parse(r);

        //
        publicationStruct.getTables().clear();

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        for (IJdxTableStruct baseStructTable : baseStruct.getTables()) {
            String baseStructTableName = baseStructTable.getName();

            // Ищем таблицу в правилах публикации
            List<String> publicationFields = null;
            for (int i = 0; i < publicationData.size(); i++) {
                JSONObject publicationTable = (JSONObject) publicationData.get(i);
                String publicationTableName = (String) publicationTable.get("table");
                if (baseStructTableName.compareToIgnoreCase(publicationTableName) == 0) {
                    publicationFields = expandPublicationFields(baseStructTable, (String) publicationTable.get("fields"));
                }
            }

            // Нашли таблицу?
            if (publicationFields != null) {
                // Добавляем в структуру публикации
                JdxTableStruct publicationTable = new JdxTableStruct();
                publicationStruct.getTables().add(publicationTable);
                //
                for (String publicationFieldName : publicationFields) {
                    IJdxFieldStruct publicationField = baseStructTable.getField(publicationFieldName).cloneField();
                    publicationTable.getFields().add(publicationField);
                }
            }
        }

    }


    public IJdxDbStruct getData() {
        return publicationStruct;
    }


    List<String> expandPublicationFields(IJdxTableStruct table, String publicationFields) {
        List<String> res = new ArrayList<>();

        //
        // DbUtils.ID_FIELD пусть будет всегда спереди (необязательно, но... во-первых это красиво!)
        res.add(DbUtils.ID_FIELD);
        if (publicationFields.compareToIgnoreCase("*") == 0) {
            for (IJdxFieldStruct fieldStruct : table.getFields()) {
                if (fieldStruct.getName().equalsIgnoreCase(DbUtils.ID_FIELD)) {
                    continue;
                }
                res.add(fieldStruct.getName());
            }
        } else {
            String[] publicationFieldsArr = publicationFields.split(",");
            for (String publicationField : publicationFieldsArr) {
                if (publicationField.equalsIgnoreCase(DbUtils.ID_FIELD)) {
                    continue;
                }
                res.add(publicationField);
            }
        }

        //
        return res;
    }

    // todo: переместить отсюда куда-нибудь в утилиты
    public static String filedsToString(List<IJdxFieldStruct> fields) {
        StringBuilder sb = new StringBuilder();

        //
        for (IJdxFieldStruct f : fields) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(f.getName());
        }

        //
        return sb.toString();
    }


}
