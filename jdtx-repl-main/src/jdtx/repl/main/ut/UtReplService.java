package jdtx.repl.main.ut;

import jandcode.utils.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtReplService {

    //
    private static Log log = LogFactory.getLog("jdtx.service");

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
        int exitCode = UtRun.run(res, "cscript.exe", getVbsScript());

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
        //log.info("Service install");

        //
        String sql = UtFile.loadString("res:jdtx/repl/main/ut/UtReplService.JadatexSync.xml", "utf-16LE");
        sql = sql.replace("${WorkingDirectory}", UtRun.getAppDir());
        sql = sql.replace("${VbsScript}", getVbsScript());
        File xmlFileTmp = new File(UtRun.getAppDir() + "JadatexSync.xml");
        UtFile.saveString(sql, xmlFileTmp, "utf-16LE");

        //
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "schtasks", "/Create", "/TN", "JadatexSync", "/XML", "JadatexSync.xml");

        //
        if (exitCode == 0) {
            UtRun.printRes(res);
        } else {
            UtRun.printRes(exitCode, res);
        }

        //
        xmlFileTmp.delete();
    }


    public static void remove() throws Exception {
        System.out.println("Service remove");

        //
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, "schtasks", "/Delete", "/TN", "JadatexSync", "/f");

        //
        if (exitCode == 0) {
            UtRun.printRes(res);
        } else {
            UtRun.printRes(exitCode, res);
        }
    }




    private static String getVbsScript() {
        return "jc-run.vbs";
    }


}
