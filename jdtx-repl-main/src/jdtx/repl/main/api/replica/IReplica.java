package jdtx.repl.main.api.replica;

import org.joda.time.*;

import java.io.*;

/**
 * Блок данных. todo: возможно нет необходитости хранить возраст (номер) ВНУТРИ реплики
 */
public interface IReplica {

    IReplicaInfo getInfo();

    File getData();

    void setData(File file);

}
