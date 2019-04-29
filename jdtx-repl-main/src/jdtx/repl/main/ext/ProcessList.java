package jdtx.repl.main.ext;

import jandcode.utils.*;

import java.io.*;
import java.util.*;

/**
 */
public class ProcessList {

    public static void start() throws IOException, InterruptedException {
        System.out.println("UtFile.getWorkdir: " + UtFile.getWorkdir());

        //
        ProcessBuilder pb = new ProcessBuilder();
        String processPath = "C:/Users/Public/Documents/Jadatex.Sync/";
        pb.command("cscript.exe", "start.vbs");
        pb.directory(new File(processPath));
        pb.redirectErrorStream(true);

        //
        Process process = pb.start();

        //
        List<String> res = new ArrayList<>();
        String charset = UtConsole.getConsoleCharset();
        charset = "cp866";
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), charset));
        String line;
        while ((line = reader.readLine()) != null) {
            res.add(line);
        }

        //
        int exitVal = process.waitFor();
        System.out.println("exitVal: " + exitVal);
        for (String s : res) {
            System.out.println(s);
        }


        //
        list();
    }

    public static long list() throws IOException {
        long processId = -1;
        Map<String, String> processInfo = new HashMap<>();

        //
        Process p = Runtime.getRuntime().exec("wmic process list /format:csv");
        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream(), "cp866"));
        //
        String processLine;
        String[] processHeaders = null;
        while ((processLine = input.readLine()) != null) {
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
        input.close();

        //
        return processId;
    }

    public static void stop() throws IOException {
        long processId = list();

        if (processId == -1) {
            System.out.println("No process found");
            return;
        }

        //
        Process p = Runtime.getRuntime().exec("wmic process where processid=" + processId + " call terminate");
        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream(), "cp866"));
        String outLine;
        while ((outLine = input.readLine()) != null) {
            if (outLine.length() > 0 && outLine.contains("ReturnValue")) {
                if (outLine.contains("ReturnValue = 0;")) {
                    System.out.println("Process terminated, ProcessId: " + processId);
                } else {
                    System.out.println(outLine);
                }
            }
        }
        input.close();
    }


}
