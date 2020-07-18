package jdtx.repl.main.api;

import jandcode.utils.error.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;

public class UtDataWriter {


    IJdxTable table;
    String[] tableFromFields;
    IRefDecoder decoder;
    boolean forbidNotOwnId;


    public UtDataWriter(IJdxTable table, String tableFields, IRefDecoder decoder, boolean forbidNotOwnId) {
        this.table = table;
        this.decoder = decoder;
        this.forbidNotOwnId = forbidNotOwnId;
        this.tableFromFields = tableFields.split(",");
    }


    public void dataBinderRec_To_DataWriter_WithRefDecode(IJdxDataBinder rsTableLog, JdxReplicaWriterXml dataWriter) throws Exception {
        for (String fieldName : tableFromFields) {
            IJdxField field = table.getField(fieldName);
            Object fieldValue = rsTableLog.getValue(fieldName);

            // Защита от дурака (для snapshot): в snapshot недопустимы чужие id
            if (forbidNotOwnId) {
                if (field.isPrimaryKey()) {
                    String refTableName = table.getName();
                    long own_id = Long.valueOf(String.valueOf(fieldValue));
                    if (!decoder.is_own_id(refTableName, own_id)) {
                        throw new XError("Not own id found, tableName: " + refTableName + ", id: " + own_id);
                    }
                }
            }

            // Запись значения с проверкой/перекодировкой ссылок
            IJdxTable refTable = field.getRefTable();
            if (field.isPrimaryKey() || refTable != null) {
                // Это значение - ссылка
                String refTableName;
                if (field.isPrimaryKey()) {
                    refTableName = table.getName();
                } else {
                    refTableName = refTable.getName();
                }
                // Перекодировка ссылки
                JdxRef ref = decoder.get_ref(refTableName, Long.valueOf(String.valueOf(fieldValue)));
                dataWriter.setRecValue(fieldName, ref.toString());
            } else {
                // Это просто значение
                dataWriter.setRecValue(fieldName, fieldValue);
            }
        }
    }


}
