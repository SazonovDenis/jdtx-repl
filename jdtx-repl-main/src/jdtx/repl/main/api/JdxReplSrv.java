package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.web.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

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
        srvHandleCommonQue(mailerList, commonQue);
    }

    public void srvDispatchReplicas() throws Exception {
        srvDispatchReplicas(commonQue, mailerList, 0, 0, true);
    }

    public void srvHandleCommonQueFrom(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IJdxMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir);

        // Физически забираем данные
        srvHandleCommonQue(mailerListLocal, commonQue);
    }

    public void srvDispatchReplicasToDir(String cfgFileName, String mailDir, long age_from, long age_to, boolean doMarkDone) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IJdxMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir);

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

            //
            long queDoneAge = stateManager.getWsQueInAgeDone(wsId);
            long queMaxAge = mailer.getSrvSate("from");

            //
            log.info("srvHandleCommonQue, from.wsId: " + wsId);

            //
            long count = 0;
            for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                // Физически забираем данные с почтового сервера
                IReplica replica = mailer.receive(age, "from");
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
                log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + ", nothing to do");
            } else {
                log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + " -> " + queMaxAge + ", done count: " + count);
            }
        }
    }

    /**
     * Сервер: распределение общей очереди по рабочим станциям
     */
    private void srvDispatchReplicas(IJdxQueCommon commonQue, Map<Long, IJdxMailer> mailerList, long age_from, long age_to, boolean doMarkDone) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // Узнаем сами, если не указано - сколько у нас есть на раздачу
        long commonQueMaxNo = commonQue.getMaxNo();

        // Узнаем сами, если не указано - сколько у нас есть
        if (age_to == 0L) {
            age_to = commonQueMaxNo;
        }

        //
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IJdxMailer mailer = (IJdxMailer) en.getValue();

            //
            log.info("srvDispatchReplicas, to.wsId: " + wsId);

            // Сколько уже отправлено для этой рабочей станции
            long commonQueDoneNo_Ws = stateManager.getCommonQueDispatchDone(wsId);

            // Узнаем сами, если не указано - от какого возраста нужно отправлять для этой рабочей станции
            if (age_from == 0L) {
                age_from = commonQueDoneNo_Ws + 1;
            }

            //
            long count = 0;
            for (long no = age_from; no <= age_to; no++) {
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
                log.info("srvDispatchReplicas, to.wsId: " + wsId + ", que.age: " + commonQueDoneNo_Ws + ", nothing to do");
            } else {
                log.info("srvDispatchReplicas, to.wsId: " + wsId + ", que.age: " + commonQueDoneNo_Ws + " -> " + commonQueMaxNo + ", done count: " + count);
            }
        }
    }


    private void fillMailerListLocal(Map<Long, IJdxMailer> mailerListLocal, String cfgFileName, String mailDir) throws Exception {
        // Готовим локальный мейлер
        mailDir = UtFile.unnormPath(mailDir) + "/";

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

        //
        DataStore t = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list");

        // Готовим локальных курьеров (через папку)
        for (DataRecord rec : t) {
            long wdId = rec.getValueLong("id");

            //
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            String guidPath = (String) cfgWs.get("guid");
            guidPath = guidPath.replace("-", "/");
            cfgWs.put("mailRemoteDir", mailDir + guidPath);
            //
            IJdxMailer mailerLocal = new UtMailerLocalFiles();
            mailerLocal.init(cfgWs);

            //
            mailerListLocal.put(wdId, mailerLocal);
        }
    }

}
