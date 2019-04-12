package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

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
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));
        //
        String url = (String) cfgData.get("url");

        // Список активных рабочих станций
        DataStore st = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list where enabled = 1");

        // Почтовые курьеры, отдельные для каждой станции
        for (DataRecord rec : st) {
            long wsId = rec.getValueLong("id");

            // Конфиг для мейлера
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
            if (cfgWs == null) {
                throw new XError("JdxReplSrv.init: cfgWs == null, wsId: " + wsId + ", cfgFileName: " + cfgFileName);
            }
            //
            cfgWs.put("guid", rec.getValueString("guid"));
            cfgWs.put("url", url);

            // Мейлер
            IJdxMailer mailer = new UtMailerHttp();
            mailer.init(cfgWs);

            //
            mailerList.put(wsId, mailer);
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
        srvHandleCommonQue(mailerList, commonQue);
    }

    public void srvDispatchReplicas() throws Exception {
        srvDispatchReplicas(commonQue, mailerList, 0, 0, true);
    }

    public void srvHandleCommonQueFrom(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IJdxMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, 0);

        // Физически забираем данные
        srvHandleCommonQue(mailerListLocal, commonQue);
    }

    public void srvDispatchReplicasToDir(String cfgFileName, String mailDir, long age_from, long age_to, long destinationWsId, boolean doMarkDone) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IJdxMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, destinationWsId);

        // Физически отправляем данные
        srvDispatchReplicas(commonQue, mailerListLocal, age_from, age_to, doMarkDone);
    }

    /**
     * Сервер: считывание очередей рабочих станций и формирование общей очереди
     * <p>
     * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
     * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
     */
    private void srvHandleCommonQue(Map<Long, IJdxMailer> mailerList, IJdxQueCommon commonQue) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IJdxMailer mailer = (IJdxMailer) en.getValue();

            // Обрабатываем каждую станцию
            try {
                log.info("srvHandleCommonQue, from.wsId: " + wsId);

                //
                long queDoneAge = stateManager.getWsQueInAgeDone(wsId);
                long queMaxAge = mailer.getSrvSate("from");

                //
                long count = 0;
                for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                    // Физически забираем данные с почтового сервера
                    IReplica replica = mailer.receive(age, "from");
                    //
                    JdxReplicaReaderXml.readReplicaInfo(replica);

                    //
                    log.debug("replica.age: " + replica.getAge() + ", replica.wsId: " + replica.getWsId());

                    // Помещаем полученные данные в общую очередь
                    db.startTran();
                    try {
                        commonQue.put(replica);

                        //
                        stateManager.setWsQueInAgeDone(wsId, age);

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback();
                        throw e;
                    }

                    // Удаляем с почтового сервера
                    mailer.delete(age, "from");

                    //
                    count++;
                }

                //
                if (count == 0) {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + ", nothing done");
                } else {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + " -> " + queMaxAge + ", done count: " + count);
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in srvHandleCommonQue, from.wsId: " + wsId + ", error: " + e.getMessage());
                log.error(getStackTrace(e));
            }
        }
    }

    /**
     * Сервер: распределение общей очереди по рабочим станциям
     */
    private void srvDispatchReplicas(IJdxQueCommon commonQue, Map<Long, IJdxMailer> mailerList, long age_from, long age_to, boolean doMarkDone) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // До скольки раздавать
        long age_to_ws = age_to;
        if (age_to_ws == 0L) {
            // Не указано - зададим сами - все что у нас есть на раздачу
            age_to_ws = commonQue.getMaxNo();
        }

        //
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IJdxMailer mailer = (IJdxMailer) en.getValue();

            // Обрабатываем каждую станцию
            try {
                log.info("srvDispatchReplicas, to.wsId: " + wsId);

                // От какого возраста нужно отправлять для этой рабочей станции
                long age_from_ws = age_from;
                if (age_from_ws == 0L) {
                    // Не указано - зададим сами (от последней отправленной)
                    age_from_ws = stateManager.getCommonQueDispatchDone(wsId) + 1;
                }

                //
                long count = 0;
                for (long no = age_from_ws; no <= age_to_ws; no++) {
                    // Берем реплику
                    IReplica replica = commonQue.getByNo(no);

                    //
                    log.debug("replica.age: " + replica.getAge() + ", replica.wsId: " + replica.getWsId());

                    // Физически отправим реплику
                    mailer.send(replica, no, "to"); // todo это тупо - вот так копировать и перекладывать файлы из папки в папку???

                    // Отметим отправку
                    if (doMarkDone) {
                        stateManager.setCommonQueDispatchDone(wsId, no);
                    }

                    //
                    count++;
                }

                //
                mailer.ping("to");

                //
                if (count == 0) {
                    log.info("srvDispatchReplicas, to.wsId: " + wsId + ", que.age: " + age_from_ws + ", nothing done");
                } else {
                    log.info("srvDispatchReplicas, to.wsId: " + wsId + ", que.age: " + age_from_ws + " -> " + age_to_ws + ", done count: " + count);
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in srvDispatchReplicas, to.wsId: " + wsId + ", error: " + e.getMessage());
                log.error(getStackTrace(e));
            }

        }
    }

    private String getStackTrace(Exception e) {
        StringWriter swr = new StringWriter();
        PrintWriter wr = new PrintWriter(swr);
        e.printStackTrace(wr);
        return swr.getBuffer().toString();
    }


    /**
     * Готовим спосок локальных (через папку) мейлеров, отдельные для каждой станции
     */
    private void fillMailerListLocal(Map<Long, IJdxMailer> mailerListLocal, String cfgFileName, String mailDir, long destinationWsId) throws Exception {
        // Список активных рабочих станций
        String sql;
        if (destinationWsId != 0) {
            // Указана конкретная станция-получатель - выгружаем только ее, остальные пропускаем
            sql = "select * from " + JdxUtils.sys_table_prefix + "workstation_list where id = " + destinationWsId;
        } else {
            // Берем только активные
            sql = "select * from " + JdxUtils.sys_table_prefix + "workstation_list where enabled = 1";
        }
        DataStore st = db.loadSql(sql);


        // Готовим курьеров
        mailDir = UtFile.unnormPath(mailDir) + "/";

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

        //
        for (DataRecord rec : st) {
            long wdId = rec.getValueLong("id");
            String guid = rec.getValueString("guid");
            String guidPath = guid.replace("-", "/");

            // Конфиг для мейлера
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            cfgWs.put("mailRemoteDir", mailDir + guidPath);

            // Мейлер
            IJdxMailer mailerLocal = new UtMailerLocalFiles();
            mailerLocal.init(cfgWs);

            //
            mailerListLocal.put(wdId, mailerLocal);
        }
    }


}
