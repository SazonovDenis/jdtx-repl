package jdtx.repl.main;

import jandcode.app.test.*;
import org.junit.*;

import java.io.*;

public class ProcessListTest extends AppTestCase {

    @Test
    public void test1() throws Exception {
        try {
            String process;
            // getRuntime: Returns the runtime object associated with the current Java application.
            // exec: Executes the specified string command in a separate process.
            Process p = Runtime.getRuntime().exec("wmic process");
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((process = input.readLine()) != null) {
                if (process.startsWith("java.exe") && process.contains("Jadatex.Sync")) {
                    System.out.println(process);
                }
            }
            input.close();
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

}
