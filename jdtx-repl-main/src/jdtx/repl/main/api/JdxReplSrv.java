package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
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

        // Почтовые курьеры для чтения/отправки сообщений, для каждой рабочей станции
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

        // Почтовые курьеры
        for (DataRecord rec : t) {
            long wdId = rec.getValueLong("id");
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            //
            //IJdxMailer mailer = new UtMailerLocalFiles();
            IJdxMailer mailer = new UtMailerHttp();
            mailer.init(cfgWs);
            //
            mailerList.put(wdId, mailer);
        }

        // Общая очередь
        commonQue.setBaseDir((String) cfgData.get("queCommon_DirLocal"));

        // Стратегии перекодировки каждой таблицы
        String strategyCfgName = "decode_strategy";
        strategyCfgName = cfgFileName.substring(0, cfgFileName.length() - UtFile.filename(cfgFileName).length()) + strategyCfgName + ".json";
        RefDecodeStrategy.instance = new RefDecodeStrategy();
        RefDecodeStrategy.instance.init(strategyCfgName);
    }

    public void srvHandleCommonQue() throws Exception {
        UtRepl utr = new UtRepl(db);
        utr.srvHandleCommonQue(mailerList, commonQue);
    }

    public void srvDispatchReplicas() throws Exception {
        UtRepl utr = new UtRepl(db);
        utr.srvDispatchReplicas(commonQue, mailerList);
    }

    public void srvHandleCommonQueFrom(String cfgFileName, String mailFromDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IJdxMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailFromDir);

        // Физически забираем данные
        UtRepl utr = new UtRepl(db);
        utr.srvHandleCommonQue(mailerListLocal, commonQue);
    }

    public void srvDispatchReplicasTo(String cfgFileName, String mailFromDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IJdxMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailFromDir);

        // Физически отправляем данные
        UtRepl utr = new UtRepl(db);
        utr.srvDispatchReplicas(commonQue, mailerListLocal);
    }


    private void fillMailerListLocal(Map<Long, IJdxMailer> mailerListLocal, String cfgFileName, String mailFromDir) throws Exception {
        // Готовим локальный мейлер
        mailFromDir = UtFile.unnormPath(mailFromDir) + "/";

        //
        JSONObject cfgData;
        Reader r = new FileReader(cfgFileName);
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }

        //
        DataStore t = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list");

        // Готовим локальных курьеров (через папку)
        for (DataRecord rec : t) {
            long wdId = rec.getValueLong("id");

            //
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            String guidPath = (String) cfgWs.get("guid");
            guidPath = guidPath.replace("-", "/");
            cfgWs.put("mailRemoteDir", mailFromDir + guidPath);
            //
            IJdxMailer mailerLocal = new UtMailerLocalFiles();
            mailerLocal.init(cfgWs);

            //
            mailerListLocal.put(wdId, mailerLocal);
        }
    }

}
