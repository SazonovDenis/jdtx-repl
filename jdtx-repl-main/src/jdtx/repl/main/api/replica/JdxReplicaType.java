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

    public static final int SET_CFG = 60;
    public static final int SET_CFG_DONE = 61;
}
