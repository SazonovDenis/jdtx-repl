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
    private DateTime replicaDtTo;
    private Map<Long, DateTime> selfAuditData = null;

    //
    public SelfAuditDtComparer(Db db) {
        this.db = db;
    }

    /**
     * Кэшируем порцию собственного аудита
     *
     * @param tableName для этой таблицы
     * @param dt        берем возраст, старше этого
     */
    public void readSelfAuditData(String tableName, DateTime dt) throws Exception {
        this.replicaDtTo = dt;
        this.selfAuditData = new HashMap<>();

        // Читаем изменения из аудита, сделанные ПОСЛЕ даты dt
        IDbNames dbNames = db.service(DbNamesService.class);
        String auditTableName = dbNames.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        String sql = "select * from " + auditTableName + " where " + UtJdx.AUDIT_FIELD_PREFIX + "OPR_DTTM > :replicaDt order by " + UtJdx.AUDIT_FIELD_PREFIX + "OPR_DTTM";
        DbQuery query = db.openSql(sql, UtCnv.toMap("replicaDt", dt));

        // Кэшируем
        try {
            while (!query.eof()) {
                DateTime dtAudit = query.getValueDateTime(UtJdx.AUDIT_FIELD_PREFIX + "OPR_DTTM");
                long id = query.getValueLong("ID");
                selfAuditData.put(id, dtAudit);
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
