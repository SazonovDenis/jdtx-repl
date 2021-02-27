package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public class PublicationRule implements IPublicationRule {

    private String tableName;
    private Collection<IJdxField> fields = new ArrayList<>();
    private String filterExpression = null;

    public PublicationRule() {
        super();
    }

    public PublicationRule(IJdxTable table) {
        tableName = table.getName();
        fields = table.getFields();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public Collection<IJdxField> getFields() {
        return fields;
    }

    @Override
    public String getFilterExpression() {
        return filterExpression;
    }

    @Override
    public void setFilterExpression(String filterExpression) {
        this.filterExpression = filterExpression;
    }

}
