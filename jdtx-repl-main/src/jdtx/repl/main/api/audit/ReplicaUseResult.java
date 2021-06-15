package jdtx.repl.main.api.audit;

public class ReplicaUseResult {

    public boolean replicaUsed = true;
    public boolean doBreak = false;
    // Возраст своих использованных реплик (важно при восстановлении после сбоев)
    public long lastOwnAgeUsed = -1;

}
