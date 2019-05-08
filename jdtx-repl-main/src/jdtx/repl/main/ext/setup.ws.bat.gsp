@echo off 

call jc repl-service-remove

call jc repl-create -ws:${args.ws_no} -guid:${args.repl_guid}-${args.ws_guid}

call jc repl-snapshot

rem служба
call jc repl-service-install

rem для сведения
call jc repl-info

