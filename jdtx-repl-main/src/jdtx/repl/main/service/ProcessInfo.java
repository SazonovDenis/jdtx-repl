package jdtx.repl.main.service;

import java.io.*;
import java.util.*;

public class ProcessInfo extends HashMap<String, String> {

    public ProcessInfo(String processLine, String[] processHeaders) {
        String[] processLineEls = processLine.split(",");
        for (int i = 0; i < processLineEls.length; i++) {
            this.put(processHeaders[i], processLineEls[i]);
        }
    }

    public long getProcessId() {
        return Long.parseLong(get("ProcessId"));
    }

    public String getProcessName() {
        return get("ExecutablePath");
    }

    public String getProcessPath() {
        return new File(get("ExecutablePath")).getParent();
    }

}
