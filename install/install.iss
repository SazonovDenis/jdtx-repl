#define App "JadatexSync"
#define DefaultDir "Jadatex.Sync"
#define AppName "Jadatex. ������ �������������"

#define OutputDir "Z:\jdtx-repl\install"
#define SourceDir "Z:\jdtx-repl\"
#define SourceDirJre "C:\jdk1.8.0_45\jre\"



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
;DisableDirPage=true
DisableFinishedPage=true
AlwaysShowComponentsList=true

Compression=lzma/ultra
SolidCompression=true
InternalCompressLevel=ultra
;SetupIconFile=Images\PawnshopApp_install_icon.ico


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


[Run]
Filename: SCHTASKS; Parameters: /Delete /TN JadatexSync /f; WorkingDir: {app}
Filename: SCHTASKS; Parameters: "/Create /TN JadatexSync /TR ""{app}\run.bat"" /SC MINUTE /MO 5"; WorkingDir: {app}

[Messages]
WelcomeLabel1=[name].
WelcomeLabel2=����� ������� ��������� ������������� ������� ��� ������ ���������.
ClickNext=������� ������, ����� ���������� ������ _CFG_APP_VERSION, ��� �������, ����� �������� ���������.

StatusRunProgram=�������������� ���� ���������...

UninstalledAll=��������� %1 ���� ������� � ������ ����������.%n�� ����������� ����������� ������ ����� ������� �������� (web\WEB-INF\*) �� ���� �������. �� ������ ������� �� ��������������.


