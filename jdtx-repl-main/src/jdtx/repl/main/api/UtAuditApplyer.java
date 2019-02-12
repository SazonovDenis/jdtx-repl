package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import java.util.*;

/**
 * Применяет реплики
 */
public class UtAuditApplyer {

    Db db;
    IJdxDbStruct struct;
    //RefDecoder decoder;

    public UtAuditApplyer(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
    }

    public void applyReplica(IReplica replica, IPublication publication, Object ws) throws Exception {
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
        ReplicaReaderXml replicaReader = new ReplicaReaderXml(replica);
        System.out.println("DbId = " + replicaReader.getDbId());

        //
        IRefDecoder decoder = new RefDecoder(db, replicaReader.getDbId()); //todo: стратегия работы с рабочими станциями в какой момент и кто задает станцию

        //
        db.startTran();


        //
        try {
            trm.setTriggersOff();


            //
            String tableName = replicaReader.nextTable();
            while (tableName != null) {
                System.out.println("table [" + tableName + "]");

                // Поиск таблицы tableName в структуре, только в одну сторону (из-за зависимостей)
                int n = -1;
                for (int i = tIdx; i < tables.size(); i++) {
                    if (tables.get(i).getName().compareToIgnoreCase(tableName) == 0) {
                        n = i;
                        break;
                    }
                }
                if (n == -1) {
                    throw new XError("table [" + tableName + "] found in replica, but not found in struct");
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

                // Перебираем записи
                Map recValues = replicaReader.nextRec();
                while (recValues != null) {
                    System.out.println("recValues=" + recValues);

                    String[] tableFromFields = publicationFields.split(",");
                    for (String fieldName : tableFromFields) {
                        IJdxFieldStruct field = table.getField(fieldName);
                        IJdxTableStruct refTable = field.getRefTable();
                        if (field.isPrimaryKey() || refTable != null) {
                            // Ссылка
                            String refTableName;
                            if (field.isPrimaryKey()) {
                                refTableName = table.getName();
                            } else {
                                refTableName = refTable.getName();
                            }
                            long ref_db = Long.valueOf((String) recValues.get(fieldName));
                            // Перекодировка ссылки
                            long ref_own = decoder.getOrCreate_id_own(ref_db, refTableName);
                            recValues.put(fieldName, ref_own);
                        } else {
                            recValues.put(fieldName, recValues.get(fieldName));
                        }
                    }

                    int oprType = Integer.valueOf((String) recValues.get("Z_OPR"));
                    if (oprType == JdxOprType.OPR_INS) {
                        try {
                            dbu.insertRec(table.getName(), recValues, publicationFields, null);
                        } catch (Exception e) {
                            if (isPrimaryKeyError(e.getCause().getMessage())) {
                                dbu.updateRec(table.getName(), recValues, publicationFields, null);
                            } else {
                                throw (e);
                            }
                        }
                    } else if (oprType == JdxOprType.OPR_UPD) {
                        dbu.updateRec(table.getName(), recValues, publicationFields, null);
                    } else if (oprType == JdxOprType.OPR_DEL) {
                        dbu.deleteRec(table.getName(), (Long) recValues.get("id"));
                    }

                    //
                    recValues = replicaReader.nextRec();
                }

                //
                tableName = replicaReader.nextTable();
            }


            //
            trm.setTriggersOn();

            //
            db.commit();
        } finally {
            if (!trm.triggersIsOn()) {
                trm.setTriggersOn();
            }
            replicaReader.close();
        }

    }

    private boolean isPrimaryKeyError(String message) {
        return message.contains("violation of PRIMARY or UNIQUE KEY constraint");
    }


}
