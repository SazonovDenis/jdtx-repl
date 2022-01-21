package jdtx.repl.main.api.audit;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.util.*;
import org.joda.time.*;

import java.util.*;

/**
 * Проверятель возраста записей (что обновляемые записи из реплики НЕ были ещё раз изменены после формирования реплики).
 * По наличию более поздних изменений в нашем аудите.
 */
public class SelfAuditDtComparer {

    private Db db;
    private String tableName;
    private DateTime replicaDtTo;
    private Map<Long, DateTime> selfAuditData = null;

    //
    public SelfAuditDtComparer(Db db) {
        this.db = db;
    }

    /**
     * Кэшируем порцию собственного аудита
     *
     * @param tableName   для этой таблицы
     * @param replicaDtTo берем возраст, старше этого
     */
    public void readSelfAuditData(String tableName, DateTime replicaDtTo) throws Exception {
        this.tableName = tableName;
        this.replicaDtTo = replicaDtTo;
        this.selfAuditData = new HashMap<>();

        // Читаем изменения из аудита, сделанные ПОСЛЕ даты replicaDtTo
        String sql = "select * from " + UtJdx.AUDIT_TABLE_PREFIX + tableName + " where Z_OPR_DTTM > :replicaDt order by Z_OPR_DTTM";
        DbQuery query = db.openSql(sql, UtCnv.toMap("replicaDt", replicaDtTo));

        // Кэшируем
        try {
            while (!query.eof()) {
                DateTime dt = query.getValueDateTime("Z_OPR_DTTM");
                long id = query.getValueLong("ID");
                selfAuditData.put(id, dt);
                //
                query.next();
            }
        } finally {
            query.close();
        }
    }

    /**
     * Проверяем, что запись replicaRecId НЕ была ещё раз изменена
     *
     * @return true, если запись БЫЛА ещё раз изменена
     */
    public boolean isSelfAuditAgeAboveReplicaAge(long replicaRecId) {
        if (selfAuditData == null) {
            return false;
        }
        DateTime recSelfAuditDt = selfAuditData.get(replicaRecId);
        if (recSelfAuditDt == null) {
            return false;
        }
        // Дата изменения записи replicaRecId в нашем аудите больше, чем дата последнего изменения из реплики
        return recSelfAuditDt.compareTo(replicaDtTo) > 0;
    }


}
