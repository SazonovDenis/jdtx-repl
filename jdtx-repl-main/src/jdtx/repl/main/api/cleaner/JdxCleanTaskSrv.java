package jdtx.repl.main.api.cleaner;

/**
 * Что можно уже удалять на сервере (ненужный аудит и обработанные реплики)
 */
public class JdxCleanTaskSrv {

    public long queInNo = -1;
    public long queOut000No = -1;
    public long queOut001No = -1;

    @Override
    public String toString() {
        return "queIn.no: " + queInNo + ", queOut000.no: " + queOut000No + ", queOut001.no: " + queOut001No;
    }
}
