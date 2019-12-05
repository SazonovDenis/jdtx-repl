package jdtx.repl.main.ut;

import jandcode.utils.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

public class UtRun {

    private static Log log = LogFactory.getLog("jdtx.UtRun");

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
        String argsStr = "";
        for (String s : args) {
            argsStr = argsStr + s + " ";
        }
        log.info("--- run:");
        log.info(argsStr);
    }


    static void printRes(long exitCode, List<String> res) {
        log.info("--- run exit code:");
        log.info(exitCode);
        printRes(res);
    }


    static void printRes(List<String> res) {
        log.info("--- run res:");
        for (String outLine : res) {
            log.info(outLine);
        }
        log.info("---");
    }


    public static String getAppDir() {
        String dir = UtFile.getWorkdir().getAbsolutePath();
        dir = UtFile.unnormPath(dir) + "\\";
        return dir;
    }


}
