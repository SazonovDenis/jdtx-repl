package jdtx.repl.main.api;

/**
 * Умеет читатть "логическую" версию базы данных.
 * Репликатором никак не используется, просто для удобства мониторинга.
 */
public interface IUtAppDbVersionReader {

    String readDbVersion() throws Exception;

}
