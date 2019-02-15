package jdtx.repl.main.api;

import java.io.*;

/**
 * Реплика в файле.
 */
public class ReplicaFile implements IReplica {

    File file = null;
    long age = -1;

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public File getFile() {
        return this.file;
    }

}
