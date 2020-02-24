package jdtx.repl.main.ut;

import jandcode.utils.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtReplService {

    //
    private static Log log = LogFactory.getLog("jdtx.Service");

    public static long list() throws Exception {
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
                            //System.out.println(processHeaders[i] + ": " + processLineEls[i]);
                            processInfo.put(processHeaders[i], processLineEls[i]);
                        }
                        processId = Long.valueOf(processInfo.get("ProcessId"));
                        System.out.println("ProcessId: " + processInfo.get("ProcessId") + ", ExecutablePath: " + processInfo.get("ExecutablePath"));
                    }
                }
            }
        } else {
            UtRun.printRes(exitCode, res);
        }


        //
        if (processId == -1) {
            System.out.println("No running process found");
        }

        //
        return processId;
    }


    public static void start() throws Exception {
        System.out.println("Process start");

        //
        System.out.println("UtFile.getWorkdir: " + UtFile.getWorkdir());

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
        list();
    }


    public static void stop() throws Exception {
        System.out.println("Process stopping");

        //
        long processId = list();
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
                        System.out.println("Process terminated, ProcessId: " + processId);
                    } else {
                        System.out.println(outLine);
                    }
                }
            }
        } else {
            UtRun.printRes(exitCode, res);
        }
    }


    public static void install() throws Exception {
        System.out.println("Service install");

        // Задача "Запуск процесса"
        String s1 = UtFile.loadString("res:jdtx/repl/main/ut/UtReplService.JadatexSync.xml", "utf-16LE");
        s1 = s1.replace("${WorkingDirectory}", UtRun.getAppDir());
        s1 = s1.replace("${VbsScript}", getVbsScriptStart());
        File xmlFileTmp1 = new File(UtRun.getAppDir() + "JadatexSync.xml");
        UtFile.saveString(s1, xmlFileTmp1, "utf-16LE");

        //
        List<String> res1 = new ArrayList<>();
        int exitCode1 = UtRun.run(res1, "schtasks", "/Create", "/TN", "JadatexSync", "/XML", "JadatexSync.xml");

        //
        if (exitCode1 == 0) {
            UtRun.printRes(res1);
        } else {
            UtRun.printRes(exitCode1, res1);
        }

        //
        xmlFileTmp1.delete();

        // Задача "Перезапуск процесса"
        String s2 = UtFile.loadString("res:jdtx/repl/main/ut/UtReplService.JadatexSyncWatchdog.xml", "utf-16LE");
        s2 = s2.replace("${WorkingDirectory}", UtRun.getAppDir());
        s2 = s2.replace("${VbsScript}", getVbsScriptStop());
        File xmlFileTmp2 = new File(UtRun.getAppDir() + "JadatexSyncWatchdog.xml");
        UtFile.saveString(s2, xmlFileTmp2, "utf-16LE");

        //
        List<String> res2 = new ArrayList<>();
        int exitCode2 = UtRun.run(res2, "schtasks", "/Create", "/TN", "JadatexSyncWatchdog", "/XML", "JadatexSyncWatchdog.xml");

        //
        if (exitCode2 == 0) {
            UtRun.printRes(res2);
        } else {
            UtRun.printRes(exitCode2, res2);
        }

        //
        xmlFileTmp2.delete();
    }


    public static void remove() throws Exception {
        System.out.println("Service remove");

        // Задача "Запуск процесса"
        List<String> res = new ArrayList<>();
        int exitCode1 = UtRun.run(res, "schtasks", "/Delete", "/TN", "JadatexSync", "/f");

        //
        if (exitCode1 == 0) {
            UtRun.printRes(res);
        } else {
            UtRun.printRes(exitCode1, res);
        }

        // Задача "Перезапуск процесса"
        List<String> res2 = new ArrayList<>();
        int exitCode2 = UtRun.run(res, "schtasks", "/Delete", "/TN", "JadatexSyncWatchdog", "/f");

        //
        if (exitCode2 == 0) {
            UtRun.printRes(res2);
        } else {
            UtRun.printRes(exitCode2, res2);
        }
    }


    private static String getVbsScriptStart() {
        return "jc-start.vbs";
    }

    private static String getVbsScriptStop() {
        return "jc-stop.vbs";
    }


}
