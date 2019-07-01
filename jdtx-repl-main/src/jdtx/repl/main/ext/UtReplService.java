package jdtx.repl.main.ext;

import jandcode.utils.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtReplService {


    public static long list() throws Exception {
        long processId = -1;
        Map<String, String> processInfo = new HashMap<>();

        //
        List<String> res = new ArrayList<>();
        int exitCode = run(res, "wmic", "process", "list", "/format:csv");

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
            printRes(exitCode, res);
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
        int exitCode = run(res, "cscript.exe", getVbsScript());

        //
        if (exitCode == 0) {
            printRes(res);
        } else {
            printRes(exitCode, res);
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
        int exitCode = run(res, "wmic", "process", "where", "\"processid=" + processId + "\"", "call", "terminate");

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
            printRes(exitCode, res);
        }
    }


    public static void install() throws Exception {
        System.out.println("Service install");
        //System.out.println("UtFile.getWorkdir: " + UtFile.getWorkdir());

        String sql = UtFile.loadString("res:jdtx/repl/main/ext/JadatexSync.xml", "utf-16LE");
        sql = sql.replace("${WorkingDirectory}", getAppDir());
        sql = sql.replace("${VbsScript}", getVbsScript());
        File xmlFileTmp = new File(getAppDir() + "JadatexSync.xml");
        UtFile.saveString(sql, xmlFileTmp, "utf-16LE");

        //
        List<String> res = new ArrayList<>();
        int exitCode = run(res, "schtasks", "/Create", "/TN", "JadatexSync", "/XML", "JadatexSync.xml");

        //
        if (exitCode == 0) {
            printRes(res);
        } else {
            printRes(exitCode, res);
        }

        //
        xmlFileTmp.delete();
    }


    public static void remove() throws Exception {
        System.out.println("Service remove");

        //
        List<String> res = new ArrayList<>();
        int exitCode = run(res, "schtasks", "/Delete", "/TN", "JadatexSync", "/f");

        //
        if (exitCode == 0) {
            printRes(res);
        } else {
            printRes(exitCode, res);
        }
    }


    public static int run(List<String> res, String... args) throws Exception {
        printArgs(args);

        //
        ProcessBuilder pb = new ProcessBuilder();
        String processPath = getAppDir();
        pb.command(args);
        pb.directory(new File(processPath));
        pb.redirectErrorStream(true);

        //
        Process process = pb.start();

        //
        if (res == null) {
            res = new ArrayList<>();
        }
        //String charset = UtConsole.getConsoleCharset();
        String charset = "cp866";
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), charset));
        String line;
        while ((line = reader.readLine()) != null) {
            res.add(line);
        }

        //
        int exitCode = process.waitFor();

        //
        return exitCode;
    }


    private static void printArgs(String... args) {
        for (String s : args) {
            System.out.print(s + " ");
        }
        System.out.println("");
    }


    static void printRes(long exitCode, List<String> res) {
        System.out.println("exitCode: " + exitCode);
        printRes(res);
    }


    static void printRes(List<String> res) {
        for (String outLine : res) {
            System.out.println(outLine);
        }
    }


    private static String getAppDir() {
        String dir = UtFile.getWorkdir().getAbsolutePath();
        dir = UtFile.unnormPath(dir) + "\\";
        //dir = "C:/Users/Public/Documents/Jadatex.Sync/";
        //System.out.println("getAppDir: " + dir);
        return dir;
    }


    private static String getVbsScript() {
        return "jc-run.vbs";
    }


}
