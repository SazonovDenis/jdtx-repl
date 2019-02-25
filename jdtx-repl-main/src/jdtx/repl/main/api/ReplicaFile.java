package jdtx.repl.main.api;

import java.io.*;

/**
 * Реплика в файле.
 */
public class ReplicaFile implements IReplica {

    File file = null;
    long wsId = -1;
    long age = -1;
    long no = -1;

    public long getWsId() {
        return wsId;
    }

    public void setWsId(long wsId) {
        this.wsId = wsId;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public long getNo() {
        return no;
    }

    public void setNo(long no) {
        this.no = no;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public File getFile() {
        return this.file;
    }

}
