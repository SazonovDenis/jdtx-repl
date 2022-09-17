package jdtx.repl.main.api.struct;

public interface IDbDatatypeManager {

    JdxDataType dbDatatypeToJdxDatatype(String dbDataType, int columnSize, int decimalDigits) throws Exception;

}
