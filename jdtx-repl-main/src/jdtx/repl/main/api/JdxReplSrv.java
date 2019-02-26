package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

// todo: исправить: как то удалось отправить три реплики на посту, хтя было всего две, путем останова на середине

/**
 * Контекст сервера
 */
public class JdxReplSrv {

    // Общая очередь на сервере
    IJdxQueCommon commonQue;

    // Источник для чтения/отправки сообщений всех рабочих станций
    Map<Long, IJdxMailer> mailerList;


    //
    Db db;

    //
    protected static Log log = LogFactory.getLog("jdtx");

    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;

        // Общая очередь на сервере
        commonQue = new JdxQueCommonFile(db, JdxQueType.COMMON);

        // Источник для чтения/отправки сообщений всех рабочих станций
        mailerList = new HashMap<>();
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
        DataStore t = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list");

        //
        for (DataRecord rec : t) {
            // Очереди рабочих станций
            long wdId = rec.getValueLong("id");
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            //
            IJdxMailer mailer = new UtMailerLocalFiles();
            mailerList.put(wdId, mailer);
            mailer.init(cfgWs);
        }

        // Общая очередь
        commonQue.setBaseDir((String) cfgData.get("queCommon_DirLocal"));
    }

    public void srvFillCommonQue() throws Exception {
        UtRepl utr = new UtRepl(db);
        utr.srvFillCommonQue(mailerList, commonQue);
    }

    public void srvDispatchReplicas() throws Exception {
        //UtRepl utr = new UtRepl(db);
        //utr.srvDispatchReplicas(commonQue, mailerList);
    }

}
