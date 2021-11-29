package jdtx.repl.main.service;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtReplService {

    //
    private static Log log = LogFactory.getLog("jdtx.Service");

    public static Collection<ProcessInfo> processList() throws Exception {
        Collection<ProcessInfo> res = new ArrayList<>();

        //
        List<String> resRun = new ArrayList<>();
        int exitCode = UtRun.run(resRun, "wmic", "process", "list", "/format:csv");

        //
        if (exitCode == 0) {
            String[] processHeaders = null;
            for (String processLine : resRun) {
                if (processLine.length() > 0 && processHeaders == null) {
                    processHeaders = processLine.split(",");
                }

                //
                if (processLine.length() > 0 && processLine.contains("java.exe")) {
                    if (processLine.contains("label:jdtx.repl.main.task")) {
                        ProcessInfo processInfo = new ProcessInfo(processLine, processHeaders);
                        res.add(processInfo);
                    }
                }
            }
        } else {
            UtRun.printRes(exitCode, resRun);
        }

        //
        return res;
    }

    public static List<ServiceInfo> serviceList() throws Exception {
        List<ServiceInfo> res = new ArrayList<>();

        //
        List<String> resRun = new ArrayList<>();
        // cmd schtasks /Query /FO LIST /V
        int exitCode = UtRun.run(resRun, "schtasks", "/Query", "/FO", "LIST", "/V");

        //
        if (exitCode == 0) {
            List<String> resRunPortion = new ArrayList<>();
            boolean isJadatexLine = false;
            for (String serviceInfoLine : resRun) {
                if (serviceInfoLine.length() == 0 && resRunPortion.size() != 0) {
                    // Flush
                    if (isJadatexLine) {
                        ServiceInfo serviceInfo = new ServiceInfo(resRunPortion);
                        res.add(serviceInfo);
                    }
                    resRunPortion.clear();
                    isJadatexLine = false;
                } else {
                    // Накопление
                    resRunPortion.add(serviceInfoLine);
                    if (serviceInfoLine.toLowerCase().contains("jadatexsync")) {
                        isJadatexLine = true;
                    }
                }
            }

            // Последний flush
            if (isJadatexLine) {
                ServiceInfo serviceInfo = new ServiceInfo(resRunPortion);
                res.add(serviceInfo);
            }

        } else {
            UtRun.printRes(exitCode, resRun);
        }

        //
        return res;
    }


    public static void start() throws Exception {
        log.info("Process start");

        //
        log.info("UtFile.getWorkdir: " + UtFile.getWorkdir());

        //
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "cscript.exe", getVbsScriptStart());

        //
        if (exitCode == 0) {
            UtRun.printRes(res);
        } else {
            UtRun.printRes(exitCode, res);
        }

        //
        Collection<ProcessInfo> processList = UtReplService.processList();
        ProcessInfo.printList(processList);
    }


    public static void stop(boolean stopAll) throws Exception {
        Collection<ProcessInfo> processList = processList();
        if (processList.size() == 0) {
            System.out.println("No process running");
            return;
        }

        //
        if (stopAll) {
            System.out.println("Process stop all");
        } else {
            System.out.println("Process stop at: " + UtRun.getAppDir());
        }

        //
        for (ProcessInfo processInfo : processList) {
            if (stopAll || isOurProcess(processInfo)) {
                long processId = processInfo.getProcessId();
                ProcessInfo.printInfo(processInfo);
                if (stopByProcessId(processId)) {
                    System.out.println("  Stopped, " + processId);
                } else {
                    System.out.println("  NOT stopped, " + processId);
                }
            } else {
                ProcessInfo.printInfo(processInfo);
                System.out.println("  Skipped, " + processInfo.getProcessId() + ", " + processInfo.getProcessPath());
            }
        }
    }

    public static boolean isStarted() throws Exception {
        Collection<ProcessInfo> processList = processList();

        //
        for (ProcessInfo processInfo : processList) {
            if (isOurProcess(processInfo)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isOurProcess(ProcessInfo processInfo) {
        String workDir = UtFile.getWorkdir().getAbsolutePath();
        if (workDir.endsWith("WEB-INF")) {
            workDir = workDir.substring(0, workDir.length() - 8);
        }
        if (workDir.endsWith("web")) {
            workDir = workDir.substring(0, workDir.length() - 4);
        }

        String processDir = processInfo.getProcessPath();
        if (processDir.endsWith("bin")) {
            processDir = processDir.substring(0, processDir.length() - 4);
        }
        if (processDir.endsWith("jre")) {
            processDir = processDir.substring(0, processDir.length() - 4);
        }

        //
        return processDir.compareToIgnoreCase(workDir) == 0;
    }

    public static void install(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.readIdGuid();

        //
        log.info("Service install");

        // Создаем задачу "Запуск процесса"
        String s1 = UtFile.loadString("res:jdtx/repl/main/service/UtReplService.JadatexSync.xml", "utf-16LE");
        s1 = s1.replace("${WorkingDirectory}", UtRun.getAppDir());
        s1 = s1.replace("${VbsScript}", getVbsScriptStart());
        File xmlFileTmp1 = new File(UtRun.getAppDir() + "JadatexSync.xml");
        UtFile.saveString(s1, xmlFileTmp1, "utf-16LE");

        //
        List<String> res1 = new ArrayList<>();
        int exitCode1 = UtRun.run(res1, "schtasks", "/Create", "/TN", "JadatexSync" + getServiceNameSuffix(ws), "/XML", "JadatexSync.xml");

        //
        if (exitCode1 == 0) {
            UtRun.printRes(res1);
        } else {
            UtRun.printRes(exitCode1, res1);
        }

        //
        xmlFileTmp1.delete();

        // Создаем задачу "Перезапуск процесса"
        String s2 = UtFile.loadString("res:jdtx/repl/main/service/UtReplService.JadatexSyncWatchdog.xml", "utf-16LE");
        s2 = s2.replace("${WorkingDirectory}", UtRun.getAppDir());
        s2 = s2.replace("${VbsScript}", getVbsScriptStop());
        File xmlFileTmp2 = new File(UtRun.getAppDir() + "JadatexSyncWatchdog.xml");
        UtFile.saveString(s2, xmlFileTmp2, "utf-16LE");

        //
        List<String> res2 = new ArrayList<>();
        int exitCode2 = UtRun.run(res2, "schtasks", "/Create", "/TN", "JadatexSyncWatchdog" + getServiceNameSuffix(ws), "/XML", "JadatexSyncWatchdog.xml");

        //
        if (exitCode2 == 0) {
            UtRun.printRes(res2);
        } else {
            UtRun.printRes(exitCode2, res2);
        }

        //
        xmlFileTmp2.delete();
    }

    /**
     * Удаляем задачи по рабочей станции.
     */
    public static void remove(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.readIdGuid();

        //
        String serviceNameS = "JadatexSync" + UtReplService.getServiceNameSuffix(ws);
        String serviceNameWS = "JadatexSyncWatchdog" + UtReplService.getServiceNameSuffix(ws);

        // Удаляем задачу "Запуск процесса"
        removeService(serviceNameS);
        // Удаляем задачу "Перезапуск процесса"
        removeService(serviceNameWS);
    }

    /**
     * Удаляем все задачи
     */
    public static void removeAll() throws Exception {
        log.info("Service remove all");

        //
        List<ServiceInfo> serviceList = UtReplService.serviceList();

        //
        for (ServiceInfo serviceInfo : serviceList) {
            String serviceName = serviceInfo.get("ServiceName");
            removeService(serviceName);
        }
    }


    /**
     * Установлены ли задачи по рабочей станции
     */
    public static boolean isInstalled(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.readIdGuid();

        //
        String serviceNameS = "JadatexSync" + UtReplService.getServiceNameSuffix(ws);
        String serviceNameWS = "JadatexSyncWatchdog" + UtReplService.getServiceNameSuffix(ws);

        //
        return serviceExists(serviceNameS) || serviceExists(serviceNameWS);
    }


    public static ReplServiceState readServiceState(Db db) throws Exception {
        ReplServiceState serviceState = new ReplServiceState();
        serviceState.isStarted = UtReplService.isStarted();
        serviceState.isInstalled = UtReplService.isInstalled(db);
        log.info("Service state read, started: " + serviceState.isStarted + ", installed: " + serviceState.isInstalled);
        return serviceState;
    }

    public static void setServiceState(Db db, ReplServiceState serviceState) throws Exception {
        log.info("Service state set, started: " + serviceState.isStarted + ", installed: " + serviceState.isInstalled);
        if (serviceState.isStarted) {
            UtReplService.start();
            log.info("Service state set - started");
        }
        if (serviceState.isInstalled) {
            UtReplService.install(db);
            log.info("Service state set - installed");
        }
    }


    /**
     * Останавливаем задачу по имени
     */
    private static boolean stopByProcessId(long processId) throws Exception {
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "wmic", "process", "where", "\"processid=" + processId + "\"", "call", "terminate");

        //
        if (exitCode == 0) {
            for (String outLine : res) {
                if (outLine.length() > 0 && outLine.contains("ReturnValue")) {
                    if (outLine.contains("ReturnValue = 0;")) {
                        return true;
                    } else {
                        log.info(">> " + outLine);
                    }
                }
            }
        } else {
            UtRun.printRes(exitCode, res);
        }

        return false;
    }

    /**
     * Удаляем задачу по имени
     */
    private static void removeService(String serviceName) throws Exception {
        log.info("Service remove: " + serviceName);

        List<String> res1 = new ArrayList<>();
        int exitCode1 = UtRun.run(res1, "schtasks", "/Delete", "/TN", serviceName, "/f");

        //
        if (exitCode1 == 0) {
            UtRun.printRes(res1);
        } else {
            if (!serviceExists(serviceName)) {
                log.info("Service not installed: " + serviceName);
            }

            UtRun.printRes(exitCode1, res1);
        }
    }

    private static boolean serviceExists(String serviceName) throws Exception {
        List<ServiceInfo> serviceList = UtReplService.serviceList();
        for (ServiceInfo service : serviceList) {
            if (service.get("ServiceName").compareTo(serviceName) == 0) {
                return true;
            }
        }
        return false;
    }

    private static String getServiceNameSuffix(JdxReplWs ws) {
        return "-" + UtString.md5Str(ws.getWsGuid()).substring(0, 8) + "-" + ws.getWsId();
    }


    private static String getVbsScriptStart() {
        return "jc-start.vbs";
    }

    private static String getVbsScriptStop() {
        return "jc-stop.vbs";
    }


}
