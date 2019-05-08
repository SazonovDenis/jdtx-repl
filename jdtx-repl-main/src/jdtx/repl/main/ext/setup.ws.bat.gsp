@echo off 

call jc repl-service-remove

call jc repl-create -ws:${args.no} -guid:${args.guid_repl}-${args.guid}

call jc repl-snapshot

rem служба
call jc repl-service-install

rem для сведения
call jc repl-info

