package jdtx.repl.main.api.replica;

/**
 * Виды реплики
 */
public class JdxReplicaType {

    public static final int SNAPSHOT = 1;
    public static final int IDE = 10;
    //
    public static final int MUTE = 20;
    public static final int MUTE_DONE = 21;
    public static final int UNMUTE = 30;
    public static final int UNMUTE_DONE = 31;
    //
    public static final int UPDATE_APP = 40;
    public static final int UPDATE_APP_DONE = 41;
    //
    public static final int SET_DB_STRUCT = 50;
    public static final int SET_DB_STRUCT_DONE = 51;
    //
    public static final int SET_CFG = 60;
    public static final int SET_CFG_DONE = 61;
    //
    public static final int SET_STATE = 75;
    public static final int SET_STATE_DONE = 76;
    //
    public static final int REPAIR_GENERATORS = 78;
    public static final int REPAIR_GENERATORS_DONE = 79;
    //
    public static final int SEND_SNAPSHOT = 80;
    public static final int SEND_SNAPSHOT_DONE = 81;
    //
    public static final int MERGE = 90;
    public static final int IDE_MERGE = 91;


    //
    public static boolean isSysReplica(int replicaType) {
        return replicaType == JdxReplicaType.MUTE ||
                replicaType == JdxReplicaType.MUTE_DONE ||
                replicaType == JdxReplicaType.UNMUTE ||
                replicaType == JdxReplicaType.UNMUTE_DONE ||
                replicaType == JdxReplicaType.SET_DB_STRUCT ||
                replicaType == JdxReplicaType.SET_DB_STRUCT_DONE ||
                replicaType == JdxReplicaType.UPDATE_APP ||
                replicaType == JdxReplicaType.UPDATE_APP_DONE ||
                replicaType == JdxReplicaType.SET_CFG ||
                replicaType == JdxReplicaType.SET_CFG_DONE ||
                replicaType == JdxReplicaType.SET_STATE ||
                replicaType == JdxReplicaType.REPAIR_GENERATORS ||
                replicaType == JdxReplicaType.REPAIR_GENERATORS_DONE ||
                replicaType == JdxReplicaType.SEND_SNAPSHOT ||
                replicaType == JdxReplicaType.SEND_SNAPSHOT_DONE ||
                replicaType == JdxReplicaType.MERGE ||
                replicaType == JdxReplicaType.IDE_MERGE;
    }

}
