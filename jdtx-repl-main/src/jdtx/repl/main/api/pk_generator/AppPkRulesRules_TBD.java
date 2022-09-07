package jdtx.repl.main.api.pk_generator;

/**
 * Реализация IAppPkRules для TBD.
 */
public class AppPkRulesRules_TBD extends AppPkRulesService implements IAppPkRules {

    @Override
    public String getGeneratorName(String tableName) {
        return "seq_" + tableName;
    }

}
