package jdtx.repl.main;

/**
 */

import java.io.*;

public class OpenFolder {

    public static void main(String[] args) {
        try {
            String process;
            // getRuntime: Returns the runtime object associated with the current Java application.
            // exec: Executes the specified string command in a separate process.
            Process p = Runtime.getRuntime().exec("wmic process list /format:csv");
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((process = input.readLine()) != null) {
                if (process.contains("java.exe")) {
                    System.out.println(process);
                    if (process.contains("Jadatex.Sync")) {
                        System.out.println("^^^^^");
                        System.out.println("");
                    }
                }
            }
            input.close();
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

}