package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 * Контекст сервера
 */
public class JdxReplSrv {

    // Общая очередь на сервере
    IJdxQueCommon commonQue;

    // Входящие очереди всех рабочих станций, получающих реплики с сервера
    Map<Long, IJdxQueCommon> queInList;

    // Исходящие очереди всех рабочих станций, отправляющих реплики на сервер
    Map<Long, IJdxQuePersonal> queOutList;

    //
    Db db;

    //
    protected static Log log = LogFactory.getLog("jdtx");

    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;

        // Общая очередь на сервере
        commonQue = new JdxQueCommonFile(db, JdxQueType.COMMON);

        // Входящие очереди всех рабочих станций, получающих реплики с сервера
        queInList = new HashMap<>();

        // Исходящие очереди всех рабочих станций, отправляющих реплики на сервер
        queOutList = new HashMap<>();
    }

    /**
     * Сервер, настройка
     *
     * @param cfgFileName json-файл с конфигурацией
     */
    public void init(String cfgFileName) throws Exception {
        JSONObject cfgData;
        Reader r = new FileReader(cfgFileName);
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }

        // Список рабочих станций
        DataStore t = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list where id <> 1");

        //
        for (DataRecord rec : t) {
            // Очереди рабочих станций
            long wdId = rec.getValueLong("id");
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            //
            IJdxQueCommon wsQueIn = new JdxQueCommonFile(db, JdxQueType.IN);
            queInList.put(wdId, wsQueIn);
            wsQueIn.setBaseDir((String) cfgWs.get("queIn_DirLocal"));
            //
            IJdxQuePersonal wsQueOut = new JdxQuePersonalFile(db, JdxQueType.OUT);
            queOutList.put(wdId, wsQueOut);
            wsQueOut.setBaseDir((String) cfgWs.get("queOut_DirLocal"));
        }

        //
        JSONObject cfgSrv = (JSONObject) cfgData.get(String.valueOf(1));
        commonQue.setBaseDir((String) cfgSrv.get("queCommon_DirLocal"));
    }

    public void srvFormCommonQue() throws Exception {
        UtRepl utr = new UtRepl(db);
        utr.srvFormCommonQue(queOutList, commonQue);
    }

    public void srvDispatchReplicas() throws Exception {
        UtRepl utr = new UtRepl(db);
        utr.srvDispatchReplicas(commonQue, queInList);
    }

}
