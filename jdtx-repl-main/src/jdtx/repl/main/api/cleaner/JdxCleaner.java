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

public class JdxCleaner {

    Db db;

    private static final Log log = LogFactory.getLog("jdtx.JdxCleaner");

    public JdxCleaner(Db db) {
        this.db = db;
    }

    /**
     * Возвращает информацию о состоянии применения реплик станцией
     */
    JdxQueUsedState readQueUsedStatus(IMailer mailer) throws Exception {
        JdxQueUsedState res = new JdxQueUsedState();

        JSONObject json = mailer.getData("ws.info", null);
        JSONObject jsonData = (JSONObject) json.get("data");
        res.queInUsed = UtJdxData.longValueOf(jsonData.get("in_queInNoDone"), -1L);
        res.queIn001Used = UtJdxData.longValueOf(jsonData.get("in_queIn001NoDone"), -1L);

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
    Map<Long, Long> get_WsQueOutNo_by_queCommonNo(long queCommonNo) throws Exception {
        Map<Long, Long> res = new HashMap<>();

        DataStore st = db.loadSql(getSqlQueCommon(), UtCnv.toMap("srvQueCommonNo", queCommonNo));
        for (DataRecord rec : st) {
            res.put(rec.getValueLong("wsId"), rec.getValueLong("wsQueOutNo"));
        }

        // System.out.println(getSqlQueCommon());
        // UtData.outTable(st);

        return res;
    }

    /**
     * Отправляет на рабочую станцию информащию о репликах, которые можно удалять
     */
    void sendQueUsedStatus(IMailer mailer, JdxQueUsedState queUsedStatus) throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("queOutUsed", queUsedStatus.queOutUsed);
        mailer.setData(data, "que.used.info", null);
    }

    /**
     * Удалить реплики из очереди que.
     * Если запрошено удаление из queOut, то очищается и аудит.
     *
     * @param que     очищаемая очередь
     * @param queNoTo номер, от которого и ниже будут удалены реплики
     */
    void cleanQue(IJdxQue que, long queNoTo, IJdxDbStruct struct) throws Exception {
        long queNoFrom = que.getMinNo();

        //
        log.info("cleanQue, que: " + que.getQueName() + ", que.noFrom: " + queNoFrom + ", que.noTo: " + queNoTo);

        // Если удаление из queOut, то очищается и аудит
        if (que.getQueName().equalsIgnoreCase(UtQue.QUE_OUT)) {
            DataRecord rec = db.loadSql(getSqlAge(), UtCnv.toMap("noFrom", queNoFrom, "noTo", queNoTo)).getCurRec();
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

        // Очищаем очередь
        for (long no = queNoFrom; no <= queNoTo; no++) {
            que.remove(no);
        }
    }

    private String getSqlAge() {
        return "select \n" +
                "  min(case when age > 0 then age else null end) ageMin,\n" +
                "  max(case when age > 0 then age else null end) ageMax\n" +
                "from\n" +
                "   " + UtQue.getQueTableName(UtQue.QUE_OUT) + "\n" +
                "where\n" +
                "  id >= :noFrom and id <= :noTo";
    }

    private String getSqlQueCommon() {
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
