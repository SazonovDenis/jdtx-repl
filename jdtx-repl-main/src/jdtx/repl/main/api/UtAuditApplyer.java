package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;

/**
 * Применяет реплики
 */
public class UtAuditApplyer {

    Db db;
    IJdxDbStruct struct;
    JdxRefDecoder decoder;

    public UtAuditApplyer(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
        decoder = new JdxRefDecoder(db, 1); //todo: стратегия работы с рабочими станциями в какой момент и кто задает станцию
    }

    public void applyAuditData(IReplica replica, IPublication publication, Object ws) throws Exception {
        InputStream ist = new FileInputStream(replica.getFile());

        List<IJdxTableStruct> tables = struct.getTables();
        int tIdx = 0;

        //
        DbUtils dbu = new DbUtils(db, struct);

        //
        JSONArray publicationData = publication.getData();

        //
        String publicationFields = null;
        IJdxTableStruct table = null;

        DbAuditTriggersManager trm = new DbAuditTriggersManager(db);

        //
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        XMLStreamReader reader = xmlInputFactory.createXMLStreamReader(ist, "utf-8");

        //
        db.startTran();

        //
        try {
            trm.setTriggersOff();

            //
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    if (reader.getLocalName().compareToIgnoreCase("rec") == 0) {
                        // Берем все поля publication
                        // publication ...

                        // Значения полей
                        Map values = new HashMap<>();

                        String[] tableFromFields = publicationFields.split(",");
                        for (String fieldName : tableFromFields) {
                            IJdxFieldStruct field = table.getField(fieldName);
                            IJdxTableStruct refTable = field.getRefTable();
                            // if (fieldName.compareToIgnoreCase(DbUtils.ID_FIELD) == 0) {
                            if (field.isPrimaryKey() || refTable != null) {
                                // Перекодировка ссылки
                                String refTableName;
                                if (field.isPrimaryKey()) {
                                    refTableName = table.getName();
                                } else {
                                    refTableName = refTable.getName();
                                }
                                long id_db = Long.valueOf(reader.getAttributeValue(null, fieldName));
                                long id_own = decoder.getOrCreate_id_own(id_db, refTableName);
                                values.put(fieldName, id_own);
                            } else {
                                values.put(fieldName, reader.getAttributeValue(null, fieldName));
                            }
                        }

                        int oprType = Integer.valueOf(reader.getAttributeValue(null, "Z_OPR"));
                        if (oprType == JdxOprType.OPR_INS) {
                            try {
                                dbu.insertRec(table.getName(), values, publicationFields, null);
                            } catch (Exception e) {
                                if (isPrimaryKeyError(e.getCause().getMessage())) {
                                    dbu.updateRec(table.getName(), values, publicationFields, null);
                                }
                            }
                        } else if (oprType == JdxOprType.OPR_UPD) {
                            dbu.updateRec(table.getName(), values, publicationFields, null);
                        } else if (oprType == JdxOprType.OPR_DEL) {
                            dbu.deleteRec(table.getName(), (Long) values.get("id"));
                        }

                    }
                    if (reader.getLocalName().compareToIgnoreCase("table") == 0) {
                        String tableName = reader.getAttributeValue(null, "name");
                        // Поиск таблицы tableName в структуре, только в одну сторону (из-за зависимостей)
                        int n = -1;
                        for (int i = tIdx; i < tables.size(); i++) {
                            if (tables.get(i).getName().compareToIgnoreCase(tableName) == 0) {
                                n = i;
                                break;
                            }
                        }
                        if (n == -1) {
                            throw new XError("table [" + tableName + "] not found");
                        }
                        tIdx = n;
                        table = tables.get(n);

                        // Поиск полей таблицы в публикации (поля берем именно из нее)
                        for (int i = 0; i < publicationData.size(); i++) {
                            JSONObject publicationTable = (JSONObject) publicationData.get(i);
                            String publicationTableName = (String) publicationTable.get("table");
                            if (table.getName().compareToIgnoreCase(publicationTableName) == 0) {
                                publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                                break;
                            }
                        }
                    }
                }
            }

            //
            trm.setTriggersOn();

            //
            db.commit();
        } finally {
            if (!trm.triggersIsOn()) {
                trm.setTriggersOn();
            }
            reader.close();
        }

    }

    private boolean isPrimaryKeyError(String message) {
        return message.contains("violation of PRIMARY or UNIQUE KEY constraint");
    }


}
