<%@ page import="jdtx.repl.main.api.UtCfgType" %>
@echo off



rem удаление старого - служба
call jc repl-service-remove -csc

rem удаление старого - данные
rmdir /Q /S %cd%\web\WEB-INF\data



rem рабочая станция "${args.ws_name}"
call jc repl-create -ws:${args.ws_list[0].ws_no} -guid:${args.repl_guid}-${args.ws_list[0].ws_guid} -file:"cfg/ws.json"



rem сервер

rem напрямую задаем структуру публикаций
call jc repl-set-cfg -cfg:${UtCfgType.PUBLICATIONS} -file:"cfg/publication_up_152_srv.json"



rem добавляем рабочие станции
<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-add-ws -ws:${args.ws_list[i].ws_no} -guid:${args.repl_guid}-${args.ws_list[i].ws_guid} -name:"${args.ws_list[i].ws_name}"
<% } %>



rem активируем рабочие станции
<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-enable -ws:${args.ws_list[i].ws_no}
<% } %>



rem создаем ящики рабочих станций
call jc repl-mail-check -create:true



rem сразу рассылаем настройки для всех станций
call jc repl-send-cfg -cfg:${UtCfgType.DECODE} -file:"cfg/decode_strategy.json"
call jc repl-send-cfg -ws:1 -cfg:${UtCfgType.PUBLICATIONS} -file:"cfg/publication_up_152_srv.json"
<% for (int i = 1; i < args.ws_list.size; i++) { %>
call jc repl-send-cfg -ws:${args.ws_list[i].ws_no} -cfg:${UtCfgType.PUBLICATIONS} -file:"cfg/publication_up_152_ws.json"
<% } %>



rem сразу инициируем фиксацию структуры БД
call jc repl-dbstruct-finish



rem служба
call jc repl-service-install


rem для сведения
call jc repl-info

