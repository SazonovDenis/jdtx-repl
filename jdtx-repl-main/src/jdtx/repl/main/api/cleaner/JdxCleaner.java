package jdtx.repl.main.api.cleaner;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.util.*;

/**
 * Набор утилитных методов для поиска и удаления старых реплик и старого аудита.
 */
public class JdxCleaner {

    Db db;

    private static final Log log = LogFactory.getLog("jdtx.JdxCleaner");

    public JdxCleaner(Db db) {
        this.db = db;
    }

    static String INFO_DATA_NAME = "ws.info";
    static String TASK_DATA_NAME = "que.used.task";

    /**
     * Читает информацию о применении реплик рабочей станцией
     */
    public JdxQueUsedState readQueUsedStatus(IMailer mailer) throws Exception {
        JSONObject json = mailer.getData(INFO_DATA_NAME, null);
        JSONObject jsonData = (JSONObject) json.get("data");

        //
        JdxQueUsedState res = new JdxQueUsedState();
        res.queInUsed = UtJdxData.longValueOf(jsonData.get("in_queInNoDone"), -1L);
        res.queIn001Used = UtJdxData.longValueOf(jsonData.get("in_queIn001NoDone"), -1L);

        //
        return res;
    }

    /**
     * Отправляет информащию о репликах, которые можно удалять на рабочей станции
     */
    public void sendQueCleanTask(IMailer mailer, JdxCleanTaskWs cleanTask) throws Exception {
        Map data = new HashMap<>();
        cleanTask.toMap(data);
        mailer.setData(data, TASK_DATA_NAME, null);
    }

    /**
     * Читает, какие реплики можно уже удалять на рабочей станции
     */
    public JdxCleanTaskWs readQueCleanTask(IMailer mailer) throws Exception {
        JSONObject json = mailer.getData(TASK_DATA_NAME, null);
        JSONObject jsonData = (JSONObject) json.get("data");

        //
        JdxCleanTaskWs res = new JdxCleanTaskWs();
        res.fromJson(jsonData);

        //
        return res;
    }

    /**
     * По номеру реплики из серверной ОБЩЕЙ очереди, которую приняли и использовали все рабочие станции,
     * для каждой рабочей станции узнаем, какой номер ИСХОДЯЩЕЙ очереди рабочей станции
     * уже принят и использован всеми другими станциями.
     * <p>
     * Т.е. определяем, до какого номера ИСХОДЯЩИЕ реплики со станци (ws.queOut.no)
     * попали в СЕРВЕРНУЮ общую очередь указанного номера (srv.queCommon.no == queCommonNo).
     */
    public Map<Long, Long> get_WsQueOutNo_by_queCommonNo(long queCommonNo) throws Exception {
        Map<Long, Long> res = new HashMap<>();

        DataStore st = db.loadSql(sqlQueCommonAuthorsIds(), UtCnv.toMap("srvQueCommonNo", queCommonNo));
        for (DataRecord rec : st) {
            res.put(rec.getValueLong("wsId"), rec.getValueLong("wsQueOutNo"));
        }

        return res;
    }

    /**
     * Удалить реплики из очереди que.
     * Если запрошено удаление из queOut, то очищается и аудит.
     *
     * @param que     очищаемая очередь
     * @param queNoTo номер, от которого и ниже будут удалены реплики
     */
    public void cleanQue(IJdxQue que, long queNoTo) throws Exception {
        long queNoFrom = que.getMinNo();

        //
        if (queNoFrom == -1 || queNoFrom > queNoTo) {
            log.info("cleanQue, que: " + que.getQueName() + ", nothing to clean");
            return;
        }
        log.info("cleanQue, que: " + que.getQueName() + ", que.noFrom: " + queNoFrom + ", que.noTo: " + queNoTo);

        // Очищаем очередь
        for (long no = queNoFrom; no <= queNoTo; no++) {
            que.remove(no);
        }
    }

    /**
     * Очистить таблицы аудита, опираясь на номер исходящей очереди.
     *
     * @param queOutNo номер в очереди queOut, по возрасту реплики из нее узнаем возраст очищаемого аудита
     * @param struct
     */
    public void cleanAudit(long queOutNo, IJdxDbStruct struct) throws Exception {
        log.info("cleanAudit, queOut.no: " + queOutNo);

        // По номеру реплики в очереди queOut, узнаем возраст age очищаемого аудита
        DataRecord rec = db.loadSql(sqlAgeByQueOutNo(), UtCnv.toMap("queOutNo", queOutNo)).getCurRec();
        long ageFrom = rec.getValueLong("ageMin");
        long ageTo = rec.getValueLong("ageMax");

        //
        if (ageFrom == 0 || ageTo == 0) {
            log.info("clearAudit, no audit found, age.from: " + ageFrom + ", age.to: " + ageTo);
            return;
        }

        //
        UtAuditSelector auditSelector = new UtAuditSelector(db, struct);
        auditSelector.clearAuditData(ageFrom, ageTo);
    }

    private String sqlAgeByQueOutNo() {
        return "select \n" +
                "  min(case when age > 0 then age else null end) ageMin,\n" +
                "  max(case when age > 0 then age else null end) ageMax\n" +
                "from\n" +
                "   " + UtQue.getQueTableName(UtQue.QUE_OUT) + "\n" +
                "where\n" +
                "  id <= :queNoTo";
    }

    private String sqlQueCommonAuthorsIds() {
        return "select\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "srv_workstation_list.id as wsId,\n" +
                "  max(author_id) as wsQueOutNo\n" +
                "from\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "srv_workstation_list\n" +
                "  left join " + UtJdx.SYS_TABLE_PREFIX + "srv_que_common on (\n" +
                "    " + UtJdx.SYS_TABLE_PREFIX + "srv_workstation_list.id = " + UtJdx.SYS_TABLE_PREFIX + "srv_que_common.author_ws_id and\n" +
                "    " + UtJdx.SYS_TABLE_PREFIX + "srv_que_common.id <= :srvQueCommonNo\n" +
                "  )\n" +
                "where\n" +
                "  1=1\n" +
                "group by\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "srv_workstation_list.id";
    }

}
