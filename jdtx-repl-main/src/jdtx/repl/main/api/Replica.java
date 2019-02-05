package jdtx.repl.main.api;

import java.io.*;

/**
 */
public class Replica implements IReplica {

    File file = null;

    public void setFile(File file) {
        this.file = file;
    }

    public File getFile() {
        return this.file ;
    }

}
