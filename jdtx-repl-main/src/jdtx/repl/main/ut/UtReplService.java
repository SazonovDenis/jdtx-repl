package jdtx.repl.main.ut;

import jandcode.utils.*;
import jdtx.repl.main.api.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtReplService {

    //
    private static Log log = LogFactory.getLog("jdtx.Service");

    public static long processList() throws Exception {
        long processId = -1;
        Map<String, String> processInfo = new HashMap<>();

        //
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "wmic", "process", "list", "/format:csv");

        //
        if (exitCode == 0) {
            String[] processHeaders = null;
            for (String processLine : res) {
                if (processLine.length() > 0 && processHeaders == null) {
                    processHeaders = processLine.split(",");
                }

                //
                if (processLine.length() > 0 && processLine.contains("java.exe")) {
                    if (processLine.contains("label:jdtx.repl.main.task")) {
                        String[] processLineEls = processLine.split(",");
                        for (int i = 0; i < processLineEls.length; i++) {
                            processInfo.put(processHeaders[i], processLineEls[i]);
                        }
                        processId = Long.valueOf(processInfo.get("ProcessId"));
                        log.info("ProcessId: " + processInfo.get("ProcessId") + ", " + processInfo.get("ExecutablePath"));
                    }
                }
            }
        } else {
            UtRun.printRes(exitCode, res);
        }


        //
        if (processId == -1) {
            log.info("No running process found");
        }

        //
        return processId;
    }

    public static void serviceList() throws Exception {
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "schtasks", "/Query", "/FO", "LIST", "/V");

        //
        List<String> pi = new ArrayList();
        boolean isJadatexTask = false;
        //
        if (exitCode == 0) {
            for (String serviceInfoLine : res) {
                if (serviceInfoLine.length() == 0 && pi.size() != 0) {
                    // Flush
                    if (isJadatexTask) {
                        printServiceInfo(pi);
                    }
                    pi.clear();
                    isJadatexTask = false;
                } else {
                    // Накопление
                    pi.add(serviceInfoLine);
                    if (serviceInfoLine.toLowerCase().contains("jadatex")) {
                        isJadatexTask = true;
                    }
                }
            }

        } else {
            UtRun.printRes(exitCode, res);
        }

        // Последний flush
        if (isJadatexTask) {
            printServiceInfo(pi);
        }
    }

    private static void printServiceInfo(List<String> pi) {
        for (String s : pi) {
            if (s.contains("Имя задачи:") ||
                    s.contains("Время прошлого запуска:") ||
                    s.contains("Задача для выполнения:") ||
                    s.contains("Рабочая папка:")
            )
                log.info(s);
        }
        log.info("---");
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
        processList();
    }


    public static void stop() throws Exception {
        log.info("Process stopping");

        //
        long processId = processList();
        if (processId == -1) {
            return;
        }

        //
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "wmic", "process", "where", "\"processid=" + processId + "\"", "call", "terminate");

        //
        if (exitCode == 0) {
            for (String outLine : res) {
                if (outLine.length() > 0 && outLine.contains("ReturnValue")) {
                    if (outLine.contains("ReturnValue = 0;")) {
                        log.info("Process terminated, ProcessId: " + processId);
                    } else {
                        log.info(">> " + outLine);
                    }
                }
            }
        } else {
            UtRun.printRes(exitCode, res);
        }
    }


    public static void install(JdxReplWs ws) throws Exception {
        log.info("Service install");

        // Создаем задачу "Запуск процесса"
        String s1 = UtFile.loadString("res:jdtx/repl/main/ut/UtReplService.JadatexSync.xml", "utf-16LE");
        s1 = s1.replace("${WorkingDirectory}", UtRun.getAppDir());
        s1 = s1.replace("${VbsScript}", getVbsScriptStart());
        File xmlFileTmp1 = new File(UtRun.getAppDir() + "JadatexSync.xml");
        UtFile.saveString(s1, xmlFileTmp1, "utf-16LE");

        //
        List<String> res1 = new ArrayList<>();
        int exitCode1 = UtRun.run(res1, "schtasks", "/Create", "/TN", "JadatexSync" + getTaskNameSuffix(ws), "/XML", "JadatexSync.xml");

        //
        if (exitCode1 == 0) {
            UtRun.printRes(res1);
        } else {
            UtRun.printRes(exitCode1, res1);
        }

        //
        xmlFileTmp1.delete();

        // Создаем задачу "Перезапуск процесса"
        String s2 = UtFile.loadString("res:jdtx/repl/main/ut/UtReplService.JadatexSyncWatchdog.xml", "utf-16LE");
        s2 = s2.replace("${WorkingDirectory}", UtRun.getAppDir());
        s2 = s2.replace("${VbsScript}", getVbsScriptStop());
        File xmlFileTmp2 = new File(UtRun.getAppDir() + "JadatexSyncWatchdog.xml");
        UtFile.saveString(s2, xmlFileTmp2, "utf-16LE");

        //
        List<String> res2 = new ArrayList<>();
        int exitCode2 = UtRun.run(res2, "schtasks", "/Create", "/TN", "JadatexSyncWatchdog" + getTaskNameSuffix(ws), "/XML", "JadatexSyncWatchdog.xml");

        //
        if (exitCode2 == 0) {
            UtRun.printRes(res2);
        } else {
            UtRun.printRes(exitCode2, res2);
        }

        //
        xmlFileTmp2.delete();
    }


    public static void remove(JdxReplWs ws) throws Exception {
        log.info("Service remove");

        // Удаляем задачу "Запуск процесса"
        List<String> res1 = new ArrayList<>();
        int exitCode1 = UtRun.run(res1, "schtasks", "/Delete", "/TN", "JadatexSync" + getTaskNameSuffix(ws), "/f");

        //
        if (exitCode1 == 0) {
            UtRun.printRes(res1);
        } else {
            UtRun.printRes(exitCode1, res1);
        }

        // Удаляем задачу "Перезапуск процесса"
        List<String> res2 = new ArrayList<>();
        int exitCode2 = UtRun.run(res2, "schtasks", "/Delete", "/TN", "JadatexSyncWatchdog" + getTaskNameSuffix(ws), "/f");

        //
        if (exitCode2 == 0) {
            UtRun.printRes(res2);
        } else {
            UtRun.printRes(exitCode2, res2);
        }
    }

    private static String getTaskNameSuffix(JdxReplWs ws) {
        return "-" + UtString.md5Str(ws.getWsGuid()).substring(0, 8) + "-" + ws.getWsId();
    }


    private static String getVbsScriptStart() {
        return "jc-start.vbs";
    }

    private static String getVbsScriptStop() {
        return "jc-stop.vbs";
    }


}
