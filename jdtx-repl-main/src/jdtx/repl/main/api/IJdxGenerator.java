package jdtx.repl.main.api;

import java.sql.*;

/**
 * ���������, ������� ���� ������������ ��� ������� ������� ��� ����������
 * ������ � ����� � ��������������
 */
public interface IJdxGenerator {

    /**
     * @param generatorName ��� ����������
     * @return ��������� id
     */
    long genId(String generatorName) throws SQLException;

}
