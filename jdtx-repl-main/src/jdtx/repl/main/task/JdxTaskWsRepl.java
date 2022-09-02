package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.repair.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "сеанс репликации" для рабочей станции.
 */
public class JdxTaskWsRepl extends JdxTaskCustom {

    //
    JdxReplWs ws;

    //
    public JdxTaskWsRepl(JdxReplWs ws) {
        this.ws = ws;
        ws.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskWs");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "ws");

        //
        logInfo("Рабочая станция, основной цикл");
        try {
            ws.init();
        } catch (Exception e) {
            if (UtJdxErrors.errorIs_failedDatabaseLock(e)) {
                log.error("ws.init: " + e.getMessage());
                return;
            }
            throw e;
        }
        logInfo("Рабочая станция, wsId: " + ws.getWsId());


        // Проверка версии приложения, обновление при необходимости
        ws.doAppUpdate();


        //
        log.info("Предварительные шаги");
        try {
            ws.wsHandleRoutineTaskIn();
        } catch (Exception e) {
            logError(e);
            collectError("ws.wsHandleRoutineTaskIn", e);
        }


        //
        logInfo("Проверяем аварийную ситуацию");
        try {
            ws.repairAfterBackupRestore(false, false);
        } catch (Exception e) {
            logError(e);
            collectError("ws.repairAfterBackupRestore", e);

            //
            logInfo("Отправка ошибок");
            IMailer mailer = ws.getMailer();
            sendErrors(mailer, "ws.errors");

            //
            logInfo("Определение команды ремонта");
            try {
                JdxRepairInfoManager repairInfoManager = new JdxRepairInfoManager(mailer);
                JdxRepairLockFileManager repairLockFileManager = new JdxRepairLockFileManager(ws.getDataDir());
                //
                String allowedRepairGuid = repairInfoManager.getAllowedRepairGuid();
                String wsRepairGuid = repairLockFileManager.repairLockFileGuid();
                //
                if (wsRepairGuid != null && wsRepairGuid.equalsIgnoreCase(allowedRepairGuid)) {
                    logInfo("Получена команда ремонта");
                    ws.repairAfterBackupRestore(true, true);
                } else {
                    logInfo("Команда ремонта не получена, allowedRepairGuid: " + allowedRepairGuid + ", wsRepairGuid: " + wsRepairGuid);
                }
            } catch (Exception eRepair) {
                logError(eRepair);
                collectError("ws.repairAfterBackupRestore", eRepair);
            }

            //
            return;
        }


        //
        logInfo("Отслеживаем и обрабатываем свои изменения");
        try {
            ws.handleSelfAudit();
        } catch (Exception e) {
            logError(e);
            collectError("ws.handleSelfAudit", e);
        }


        //
        logInfo("Отправляем свои реплики");
        try {
            ws.replicasSend();
        } catch (Exception e) {
            logError(e);
            collectError("ws.send", e);
        }


        //
        logInfo("Забираем входящие реплики");
        try {
            ws.replicasReceive();
        } catch (Exception e) {
            logError(e);
            collectError("ws.receive", e);
        }


        //
        logInfo("Применяем входящие реплики");
        try {
            ws.handleAllQueIn();
        } catch (Exception e) {
            logError(e);
            collectError("ws.handleQueIn", e);
        }


        //
        logInfo("Отправляем свои реплики (ответ на входящие)");
        try {
            ws.replicasSend();
        } catch (Exception e) {
            logError(e);
            collectError("ws.send", e);
        }


        //
        try {
            if (errorCollector.getErrors().size() == 0) {
                log.info("Очистка почтовых ящиков");
                ws.wsCleanupMailInBox();
            } else {
                log.info("Очистка почтовых ящиков пропущена");
            }
        } catch (Exception e) {
            logError(e);
            collectError("srv.wsCleanupMailInBox", e);
        }


        //
        logInfo("Отправка ошибок");
        IMailer mailer = ws.getMailer();
        sendErrors(mailer, "ws.errors");


        //
        logInfo("Рабочая станция, основной цикл завершен");
        log.info("----------");

        //
        MDC.remove("serviceName");
    }

}
