package jdtx.repl.main.api.replica;

import jdtx.repl.main.api.replica.*;

import java.io.*;

/**
 * Реализация IReplica
 * Реплика, хранение во внешнем файле.
 */
public class ReplicaFile implements IReplica {

    File file = null;
    long wsId = -1;
    long age = -1;
    int replicaType = JdxReplicaType.IDE;

    public long getWsId() {
        return wsId;
    }

    public void setWsId(long wsId) {
        this.wsId = wsId;
    }

    public long getAge() {
        return age;
    }

    public int getReplicaType() {
        return replicaType;
    }

    public void setReplicaType(int replicaType) {
        this.replicaType = replicaType;
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
