package jdtx.repl.main.service;

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
                    if (serviceInfoLine.toLowerCase().contains("jadatex")) {
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
        String workDir = UtFile.getWorkdir().getAbsolutePath();

        //
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
            String executablePath = processInfo.getProcessPath();
            if (stopAll || executablePath.compareToIgnoreCase(workDir) == 0) {
                long processId = processInfo.getProcessId();
                ProcessInfo.printInfo(processInfo);
                if (stopByProcessId(processId)) {
                    System.out.println("  Stopped, " + processId);
                } else {
                    System.out.println("  NOT stopped, " + processId);
                }
            } else {
                ProcessInfo.printInfo(processInfo);
                System.out.println("  Skipped, " + processInfo.getProcessId());
            }
        }
    }


    static boolean stopByProcessId(long processId) throws Exception {
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

    public static void install(JdxReplWs ws) throws Exception {
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
     * Удаляем задачи по рабочей станции
     */
    public static void remove(JdxReplWs ws) throws Exception {
        // Удаляем задачу "Запуск процесса"
        String serviceName = "JadatexSync" + UtReplService.getServiceNameSuffix(ws);
        removeService(serviceName);

        // Удаляем задачу "Перезапуск процесса"
        serviceName = "JadatexSyncWatchdog" + UtReplService.getServiceNameSuffix(ws);
        removeService(serviceName);
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
            UtRun.printRes(exitCode1, res1);
        }
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
