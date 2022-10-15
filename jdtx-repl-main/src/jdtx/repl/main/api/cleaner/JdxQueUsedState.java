package jdtx.repl.main.api.cleaner;

/**
 * Информация о состоянии применения реплик на рабочей станции
 */
public class JdxQueUsedState {

    public long queInUsed = -1;
    public long queIn001Used = -1;

    @Override
    public String toString() {
        return "queIn.used: " + queInUsed + ", queIn001.used: " + queIn001Used;
    }

}
