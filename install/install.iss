#define App "JadatexSync"
#define DefaultDir "Jadatex.Sync"
#define AppName "Jadatex. ������ �������������"

#define OutputDir "Z:\jdtx-repl\install"
#define SourceDir "Z:\jdtx-repl\"
//#define SourceDirJre "D:\jdk1.8.0_45\jre\"
#define SourceDirJre "D:\jdk1.8.0_202\jre\"



#define AppVersion "_CFG_APP_VERSION"
#define _CFG_INSTALL_JRE

#ifdef installJre
  #define FilenameSuffixJre = ".jre"
#else
  #define FilenameSuffixJre = ""
#endif



[Setup]
AppID=Jadatex.Sync
AppPublisher=�������� Jadatex
AppPublisherURL=http://jadatex.com
AppSupportURL=http://jadatex.com/wiki/contacts
AppUpdatesURL=http://jadatex.com/wiki/downloads_list

OutputBaseFilename={#App}-{#AppVersion}{#FilenameSuffixJre}
AppName={#AppName}
AppVerName={#App} (������ {#AppVersion})
DefaultDirName={commondocs}\{#DefaultDir}
DefaultGroupName=Jadatex PawnShop

OutputDir={#OutputDir}
SourceDir={#SourceDir}

;WizardImageFile=Images\PawnshopApp_install.bmp
;WizardSmallImageFile=Images\PawnshopApp_install_48.bmp
WizardImageStretch=false

DisableReadyPage=true
DisableStartupPrompt=true
UninstallRestartComputer=false
DisableProgramGroupPage=true
DisableDirPage=false
DisableFinishedPage=true
AlwaysShowComponentsList=true

Compression=lzma/ultra
SolidCompression=true
InternalCompressLevel=ultra
;SetupIconFile=Images\PawnshopApp_install_icon.ico

DirExistsWarning=no

PrivilegesRequired=lowest

[Languages]
Name: ru; MessagesFile: compiler:Languages\Russian.isl

[Icons]
;Name: {group}\{#AppName}; Filename: {app}\Pawn.exe; WorkingDir: {app}
;Name: {commondesktop}\{#AppName}; Filename: {app}\Pawn.exe; WorkingDir: {app}; IconIndex: 0

[_ISTool]
EnableISX=true

[LangOptions]
LanguageID=$313D



[Dirs]
;Name: {commondocs}\Jadatex.PawnShop; Flags: uninsneveruninstall
;Name: {commondocs}\Jadatex.PawnShop\ReportCustom; Flags: uninsneveruninstall
;Name: {commondocs}\Jadatex.PawnShop\Install; Flags: uninsneveruninstall



[Files]
Source: temp\distr\*.*; DestDir: {app}; Flags: ignoreversion recursesubdirs createallsubdirs
#ifdef installJre
Source: {#SourceDirJre}\*.*; DestDir: {app}\jre; Flags: ignoreversion recursesubdirs createallsubdirs
#endif



Source: install\cm.bat;                                 DestDir: {app};                 Flags: ignoreversion
Source: install\cm.bat.lnk;                             DestDir: {app};                 Flags: ignoreversion
Source: install\jc-start.bat;                           DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist
Source: install\jc-start.vbs;                           DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\jc-stop.bat;                            DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\jc-stop.vbs;                            DestDir: {app}\web\WEB-INF;     Flags: ignoreversion

Source: install\cfg\ws.json;                            DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\field_groups_194.json;              DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\decode_strategy_194.json;           DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\publication_lic_194_srv.json;       DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\publication_lic_194_ws.json;        DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\publication_lic_194_snapshot.json;  DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent


Source: install\sample.log.properties;                  DestDir: {app}\web\WEB-INF;     Flags: ignoreversion; DestName: log.properties
Source: install\sample.db-ini.rt;                       DestDir: {app}\web\WEB-INF;     Flags: ignoreversion; DestName: db-ini.rt
Source: install\sample.srv._app.rt;                     DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _app.rt; Components: ServerComponent
Source: install\sample.ws._app.rt;                      DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _app.rt; Components: ClientComponent
Source: install\sample.db-connection.xml;               DestDir: {app};                 Flags: onlyifdoesntexist; DestName: db-connection.xml

;Source: install\543.bat;                                DestDir: {app}\web\WEB-INF;     Flags: ignoreversion deleteafterinstall;
;Source: install\rename.dirs.bat;                        DestDir: {app};                 Flags: ignoreversion deleteafterinstall;



[Run]
;Filename: jc.bat; Parameters: repl-service-remove; WorkingDir: {app}
;Filename: jc.bat; Parameters: repl-service-install; WorkingDir: {app}


[UninstallRun]
Filename: jc.bat; Parameters: repl-service-remove; WorkingDir: {app}



[UninstallDelete]
Name: {app}\output.err;   Type: files
Name: {app}\output.msg;   Type: files
Name: {app}\jdtx.log;     Type: files
Name: {app}\jdtx-dbm.log; Type: files
Name: {app}\jre;          Type: filesandordirs


[Messages]
WelcomeLabel1=[name].
WelcomeLabel2=����� ������� ��������� ������������� ������� ��� ������ ���������.
ClickNext=������� ������, ����� ���������� ������ _CFG_APP_VERSION, ��� �������, ����� �������� ���������.

StatusRunProgram=�������������� ���� ���������...

UninstalledAll=��������� %1 ���� ������� � ������ ����������.%n�� ����������� ����������� ������ ������� �������� (� �������� web\WEB-INF\*) �� ���� �������. �� ������ ������� �� ��������������.



[Types]
Name: ClientInstall; Description: ������� �������
Name: ServerInstall; Description: ��������� �� ������; Flags: iscustom



[Components]
Name: ClientComponent; Description: ������������ ������� �������; Types: ClientInstall; Flags: fixed
Name: ServerComponent; Description: ��������� ������������; Types: ServerInstall; Flags: fixed



[Code]


const
  SetupMutexName = 'Jadatex.Sync';


function InitializeSetup(): Boolean;
begin
  Result := True;
  if CheckForMutexes(SetupMutexName) then
  begin
    Log('Mutex exists, setup is running already, silently aborting');
    if (not WizardSilent) then
    begin
      MsgBox('��������� '+ExpandConstant('{#AppName}')+' ��� ��������, ��������� � ����������.', mbError, MB_OK);
    end;
    Result := False;
  end
    else
  begin
    Log('Creating mutex');
    CreateMutex(SetupMutexName);
  end;
end;


procedure CurStepChanged(CurStep: TSetupStep); 
var
  resultCode: Integer;
  i: Integer;
begin
  if (CurStep=ssInstall) then
  begin
    Exec(ExpandConstant('jc.bat'), 'repl-service-stop' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-stop.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
    Exec(ExpandConstant('jc.bat'), 'repl-service-stop' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-stop.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
    //Exec(ExpandConstant('jc.bat'), 'repl-service-remove' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-remove.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
  end;


  //
  if (CurStep=ssDone) then
  begin
    for i:=1 to ParamCount() do
    begin
      if Lowercase(ParamStr(i)) = '/repl-service-install' then
      begin
        Exec(ExpandConstant('jc.bat'), 'repl-service-install' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-install.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
        Exec(ExpandConstant('jc.bat'), 'repl-service-start' + ' >> '+ ExpandConstant('{app}') + '\jdtx.repl-service-start.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
        exit;
      end;
    end;

    //MsgBox('repl-service-remove', mbInformation, MB_OK);
    Exec(ExpandConstant('jc.bat'), 'repl-service-remove' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-remove.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
  end;

end;

