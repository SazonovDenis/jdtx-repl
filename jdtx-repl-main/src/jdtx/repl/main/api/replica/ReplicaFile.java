package jdtx.repl.main.api.replica;

import org.joda.time.*;

import java.io.*;

/**
 * Реализация IReplica
 * Реплика, хранение во внешнем файле.
 */
public class ReplicaFile implements IReplica {

    private IReplicaInfo info = new ReplicaInfo();

    private File file = null;

    public IReplicaInfo getInfo() {
        return this.info;
    }

    public File getData() {
        return this.file;
    }

    public void setData(File file) {
        this.file = file;
    }

}
