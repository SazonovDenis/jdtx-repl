package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ut.*;
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
    Map<Long, IMailer> mailerList;

    //
    Db db;
    private IJdxDbStruct struct;


    //
    protected static Log log = LogFactory.getLog("jdtx");


    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;

        // Общая очередь на сервере
        commonQue = new JdxQueCommonFile(db, JdxQueType.COMMON);

        // Почтовые курьеры для чтения/отправки сообщений, для каждой рабочей станции
        mailerList = new HashMap<>();

        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
    }

    /**
     * Сервер, настройка
     *
     * @param cfgFileName json-файл с конфигурацией
     */
    public void init(String cfgFileName) throws Exception {
        // Проверка наличия в БД служебных структур и их версии
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.checkReplVerDb();

        // Проверка, что инициализация станции прошла
        ut.checkReplDb();

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

        //
        String url = (String) cfgData.get("url");

        // Список активных рабочих станций
        String sql = "select Z_Z_workstation_list.* from Z_Z_workstation_list join Z_Z_state_ws on (Z_Z_workstation_list.id = Z_Z_state_ws.ws_id) where Z_Z_state_ws.enabled = 1";
        DataStore st = db.loadSql(sql);

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
            IMailer mailer = new MailerHttp();
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


        // Проверка версии приложения
        UtAppVersion_DbRW appVersionRW = new UtAppVersion_DbRW(db);
        String appVersionAllowed = appVersionRW.getAppVersionAllowed();
        String appVersionActual = UtRepl.getVersion();
        if (appVersionAllowed.length() == 0) {
            log.warn("appVersionAllowed.length == 0, appVersionActual: " + appVersionActual);
        } else if (appVersionActual.compareToIgnoreCase("SNAPSHOT") == 0) {
            log.warn("appVersionActual == SNAPSHOT, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
        } else if (appVersionAllowed.compareToIgnoreCase(appVersionActual) != 0) {
            throw new XError("appVersionAllowed != appVersionActual, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
        }
    }

    public void addWorkstation(long wsId, String wsName, String wsGuid) throws Exception {
        log.info("add workstation, wsId: " + wsId + ", name: " + wsName);

        // workstation_list
        Map params = UtCnv.toMap(
                "id", wsId,
                "name", wsName,
                "guid", wsGuid
        );
        String sql = "insert into " + JdxUtils.sys_table_prefix + "workstation_list (id, name, guid) values (:id, :name, :guid)";
        db.execSql(sql, params);

        // state_ws
        DbUtils dbu = new DbUtils(db, null);
        long id = dbu.getNextGenerator(JdxUtils.sys_gen_prefix + "state_ws");
        sql = "insert into " + JdxUtils.sys_table_prefix + "state_ws (id, ws_id, que_common_dispatch_done, que_in_age_done, enabled, mute_age) values (" + id + ", " + wsId + ", 0, 0, 0, 0)";
        db.execSql(sql);
    }

    public void enableWorkstation(long wsId) throws Exception {
        log.info("enable workstation, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set enabled = 1 where id = " + wsId;
        db.execSql(sql);
        sql = "update " + JdxUtils.sys_table_prefix + "state set enabled = 1 where id = 1";
        db.execSql(sql);
    }

    public void disableWorkstation(long wsId) throws Exception {
        log.info("disable workstation, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set enabled = 0 where id = " + wsId;
        db.execSql(sql);
        sql = "update " + JdxUtils.sys_table_prefix + "state set enabled = 0 where id = 1";
        db.execSql(sql);
    }

    public void srvHandleCommonQue() throws Exception {
        srvHandleCommonQue(mailerList, commonQue);
    }

    public void srvDispatchReplicas() throws Exception {
        srvDispatchReplicas(commonQue, mailerList, 0, 0, true);
    }

    public void srvHandleCommonQueFrom(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, 0);

        // Физически забираем данные
        srvHandleCommonQue(mailerListLocal, commonQue);
    }

    public void srvDispatchReplicasToDir(String cfgFileName, String mailDir, long age_from, long age_to, long destinationWsId, boolean doMarkDone) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, destinationWsId);

        // Физически отправляем данные
        srvDispatchReplicas(commonQue, mailerListLocal, age_from, age_to, doMarkDone);
    }


    public void srvDbStructStart() throws Exception {
        log.info("srvDbStructStart");

        // Системная команда "MUTE" в общую очередь
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute();

        // Системная команда - в исходящую очередь реплик
        commonQue.put(replica);
    }


    public void srvDbStructFinish() throws Exception {
        log.info("srvDbStructFinish");


        // Системные команды в общую очередь
        IReplica replica;
        UtRepl utRepl = new UtRepl(db, struct);


        // Системная команда "SET_DB_STRUCT"...
        replica = utRepl.createReplicaSetDbStruct();
        // ... в исходящую очередь реплик
        commonQue.put(replica);


        // Системная команда "UNMUTE" ...
        replica = utRepl.createReplicaUnmute();
        // ...в исходящую очередь реплик
        commonQue.put(replica);
    }


    public void srvAppUpdate(String exeFileName) throws Exception {
        log.info("srvAppUpdate, exeFileName: " + exeFileName);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

        // Системная команда - в исходящую очередь реплик
        commonQue.put(replica);
    }


    public void srvSetCfg(String cfgFileName, long wsId) throws Exception {
        log.info("srvSetCfg, cfgFileName: " + cfgFileName);

        //
        DbUtils ut = new DbUtils(db, struct);

        //
        String appCfg = loadAndValidateCfgFile(cfgFileName);

        //
        db.startTran();
        try {
            // Обновляем конфиг у себя в серверной БД
            ut.updateRec("Z_Z_workstation_list", UtCnv.toMap(
                    "id", wsId,
                    "app_cfg", appCfg
            ));

/*
            ^с
            как задавать адресатов реплики "задать конфиг"
            место хранения настроек в БД (общих и персональных для каждой станции)
*/

            // Системная команда ...
            UtRepl utRepl = new UtRepl(db, struct);
            IReplica replica = utRepl.createReplicaSetCfg(cfgFileName, wsId);

            // ... в исходящую очередь реплик
            commonQue.put(replica);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }


    private String loadAndValidateCfgFile(String cfgFileName) throws Exception {
        String appCfg = UtFile.loadString(cfgFileName);
        JSONObject cfgData = (JSONObject) UtJson.toObject(appCfg);
        return UtJson.toString(cfgData);
    }


    /**
     * Сервер: считывание очередей рабочих станций и формирование общей очереди
     * <p>
     * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
     * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
     */
    private void srvHandleCommonQue(Map<Long, IMailer> mailerList, IJdxQueCommon commonQue) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Обрабатываем каждую станцию
            try {
                log.info("srvHandleCommonQue, from.wsId: " + wsId);

                //
                long queDoneAge = stateManager.getWsQueInAgeDone(wsId);
                long queMaxAge = mailer.getSrvState("from");

                //
                long count = 0;
                for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                    log.info("receive, wsId: " + wsId + ", receiving.age: " + age);

                    // Информацмия о реплике с почтового сервера
                    ReplicaInfo info = mailer.getReplicaInfo("from", age);

                    // Физически забираем данные с почтового сервера
                    IReplica replica = mailer.receive("from", age);

                    // Проверяем целостность скачанного
                    String md5file = JdxUtils.getMd5File(replica.getFile());
                    if (!md5file.equals(info.getCrc())) {
                        log.error("receive.replica: " + replica.getFile());
                        log.error("receive.replica.md5: " + md5file);
                        log.error("mailer.info.crc: " + info.getCrc());
                        // Неправильно скачанный файл - удаляем, чтобы потом начать снова
                        replica.getFile().delete();
                        // Ошибка
                        throw new XError("receive.replica.md5 <> mailer.info.crc");
                    }
                    //
                    JdxReplicaReaderXml.readReplicaInfo(replica);

                    //
                    log.debug("replica.age: " + replica.getInfo().getAge() + ", replica.wsId: " + replica.getInfo().getWsId());

                    // Помещаем полученные данные в общую очередь
                    db.startTran();
                    try {
                        // Помещаем в очередь
                        commonQue.put(replica);

                        // Отмечаем факт скачивания
                        stateManager.setWsQueInAgeDone(wsId, age);

                        // Реагируем на системные реплики
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.MUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setMuteDone(wsId, age);
                        }
                        //
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.UNMUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setUnmuteDone(wsId);
                        }

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback();
                        throw e;
                    }

                    // Удаляем с почтового сервера
                    mailer.delete("from", age);

                    //
                    count++;
                }


                //
                mailer.pingRead("from");
                //
                Map info = getInfoSrv();
                mailer.setSrvInfo(info);


                //
                if (queDoneAge <= queMaxAge) {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + " .. " + queMaxAge + ", done count: " + count);
                } else {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in srvHandleCommonQue, from.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }
        }
    }

    /**
     * Сервер: распределение общей очереди по рабочим станциям
     */
    private void srvDispatchReplicas(IJdxQueCommon commonQue, Map<Long, IMailer> mailerList, long no_from, long no_to, boolean doMarkDone) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // До скольки раздавать
        long no_to_ws = no_to;
        if (no_to_ws == 0L) {
            // Не указано - зададим сами - все что у нас есть на раздачу
            no_to_ws = commonQue.getMaxNo();
        }

        //
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Обрабатываем каждую станцию
            try {
                log.info("srvDispatchReplicas, to.wsId: " + wsId);

                // От какого возраста нужно отправлять для этой рабочей станции
                long no_from_ws = no_from;
                if (no_from_ws == 0L) {
                    // Не указано - зададим сами (от последней отправленной)
                    no_from_ws = stateManager.getCommonQueDispatchDone(wsId) + 1;
                }

                //
                long count = 0;
                for (long no = no_from_ws; no <= no_to_ws; no++) {
                    // Берем реплику
                    IReplica replica = commonQue.getByNo(no);

                    //
                    log.debug("replica.age: " + replica.getInfo().getAge() + ", replica.wsId: " + replica.getInfo().getWsId());

                    // Физически отправим реплику
                    mailer.send(replica, "to", no); // todo это тупо - вот так копировать и перекладывать файлы из папки в папку???

                    // Отметим отправку
                    if (doMarkDone) {
                        stateManager.setCommonQueDispatchDone(wsId, no);
                    }

                    //
                    count++;
                }

                //
                mailer.pingWrite("to");
                //
                Map info = getInfoSrv();
                mailer.setSrvInfo(info);

                //
                if (no_from_ws <= no_to_ws) {
                    log.info("srvDispatchReplicas, to.wsId: " + wsId + ", que.age: " + no_from_ws + " .. " + no_to_ws + ", done count: " + count);
                } else {
                    log.info("srvDispatchReplicas, to.wsId: " + wsId + ", que.age: " + no_from_ws + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in srvDispatchReplicas, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
    }

    private Map getInfoSrv() {
        return new HashMap<>();
    }


    /**
     * Готовим спосок локальных (через папку) мейлеров, отдельные для каждой станции
     */
    private void fillMailerListLocal(Map<Long, IMailer> mailerListLocal, String cfgFileName, String mailDir, long destinationWsId) throws Exception {
        // Список активных рабочих станций
        String sql;
        if (destinationWsId != 0) {
            // Указана конкретная станция-получатель - выгружаем только ее, остальные пропускаем
            sql = "select * from " + JdxUtils.sys_table_prefix + "workstation_list where id = " + destinationWsId;
        } else {
            // Берем только активные
            sql = "select Z_Z_workstation_list.* from Z_Z_workstation_list join Z_Z_state_ws on (Z_Z_workstation_list.id = Z_Z_state_ws.ws_id) where Z_Z_state_ws.enabled = 1";
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
            IMailer mailerLocal = new MailerLocalFiles();
            mailerLocal.init(cfgWs);

            //
            mailerListLocal.put(wdId, mailerLocal);
        }
    }


}
