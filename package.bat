@echo off

@rem 设置文件目录
set project=BonreeAnts
@rem 设置文件目录
set dirs=%project%\bin,%project%\jar,%project%\conf,%project%\release,%project%\logs,%project%\plugin
@rem 设置当前路径
set baseDir=%~dp0
set version=%date:~0,4%%date:~5,2%%date:~8,2%

@rem 设置需要打包的模块
set modules=Calc_Engine,Storage_Engine,Plugins,Extention_Engine

@rem 初始项目的目录结构
rd /s /q %project%
for %%i in (%dirs%) do (
	if exist %baseDir%%%i (
		echo dir '%%i' already exist.
	) else (
		md %%i
		if "%%i" == "%project%\jar" (
			for %%n in (%modules%) do md %%i\%%n
		)
		echo dir '%%i' not exist, create it.
	)
)

@rem 工程里生成版本号文件
for %%i in (%modules%) do if exist %%i\src\main\resources\ver_*.txt del /s /q %%i\src\main\resources\ver_*.txt
for %%i in (%modules%) do echo version: %version% > %%i\src\main\resources\ver_%version%.txt

@rem maven打包
call mvn clean package -Dmaven.test.skip=true

@rem 删除工程里版本号文件
for %%i in (%modules%) do if exist %%i\src\main\resources\ver_*.txt del /s /q %%i\src\main\resources\ver_*.txt

@rem 复制程序jar,配置文件,启动脚本,版本文件
for %%i in (%modules%) do copy %%i\target\%%i.jar %project%\jar\%%i\
for %%i in (%modules%) do if exist %%i\bin copy %%i\bin\*.* %project%\jar\%%i\
xcopy conf\*.* %project%\conf\ /s
xcopy bin\*.* %project%\bin\
xcopy release\*.* %project%\release\
echo %version% > %project%\ver_%version%.txt

pause
