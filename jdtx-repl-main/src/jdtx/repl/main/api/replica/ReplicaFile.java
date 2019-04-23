package jdtx.repl.main.api.replica;

import org.joda.time.*;

import java.io.*;

/**
 * Реализация IReplica
 * Реплика, хранение во внешнем файле.
 */
public class ReplicaFile implements IReplica {

    File file = null;
    long wsId = -1;
    long age = -1;
    DateTime dtFrom;
    DateTime dtTo;
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

    public void setAge(long age) {
        this.age = age;
    }

    public DateTime getDtFrom() {
        return dtFrom;
    }

    public void setDtFrom(DateTime dtFrom) {
        this.dtFrom = dtFrom;
    }

    public DateTime getDtTo() {
        return dtTo;
    }

    public void setDtTo(DateTime dtTo) {
        this.dtTo = dtTo;
    }

    public int getReplicaType() {
        return replicaType;
    }

    public void setReplicaType(int replicaType) {
        this.replicaType = replicaType;
    }

    public File getFile() {
        return this.file;
    }

    public void setFile(File file) {
        this.file = file;
    }

}
