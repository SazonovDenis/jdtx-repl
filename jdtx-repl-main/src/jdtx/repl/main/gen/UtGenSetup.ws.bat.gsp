<%@ page import="jdtx.repl.main.api.manager.CfgType" %>
@echo off

call jc -csc



rem удаление старого - служба
call jc repl-service-remove -all

rem удаление старого - данные
rmdir /Q /S %cd%\web\WEB-INF\data



rem рабочая станция "${args.ws_name}"
call jc repl-create -ws:${args.ws_no} -guid:${args.repl_guid}-${args.ws_guid} -file:"cfg/ws.json"



rem служба
call jc repl-service-install



rem для сведения
call jc repl-info
