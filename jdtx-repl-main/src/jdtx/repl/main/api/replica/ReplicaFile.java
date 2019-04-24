package jdtx.repl.main.api.replica;

import org.joda.time.*;

import java.io.*;

/**
 * Реализация IReplica
 * Реплика, хранение во внешнем файле.
 */
public class ReplicaFile implements IReplica {

    protected IReplicaInfo info = new ReplicaInfo();

    protected File file = null;

    public IReplicaInfo getInfo() {
        return this.info;
    }

    public File getFile() {
        return this.file;
    }

    public void setFile(File file) {
        this.file = file;
    }

}
