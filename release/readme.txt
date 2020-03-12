====================================【介绍】====================================
【目录结构】
    BonreeAnts/
        bin/
            start_ants.sh       #启动脚本
            stop_ants.sh        #停止脚本
        conf/
            app.xml		        #基础配置文件
            schema.xml	        #业务配置文件
            schema/  	        #各业务表的配置xml
            active/		        #计算活跃数的配置xml
            extention/	        #扩展业务(基线报警)的配置xml
        jar/
            Calc_Engine/		#数据计算相关的jar
            Extention_Engine/	#扩展业务相关(基线报警)的jar
            Plugins/			#插件相关接口的jar，此jar要提供给客户
            Storage_Engine/ 	#数据存储的jar
        logs/					#存放各计算拓扑的启动日志
        readme.txt			    #配置变更记录
        release-notes.txt		#功能变更记录
        ver_201800502.txt		#版本文件


【启动程序】
    sh start_ants.sh all 	    #启动全部拓扑(预计算/小粒度汇总/大粒度汇总/存储)
    sh start_ants.sh upload     #上传插件和Schema配置,以动态生效变更
    sh start_ants.sh baseline 	#启动基线拓扑
    sh start_ants.sh alert 	    #启动报警拓扑

【停止程序】
    sh stop_ants.sh all 	    #停止全部拓扑(扩展业务的相关拓扑除外)
    sh stop_ants.sh baseline 	#停止基线拓扑
    sh stop_ants.sh alert    	#停止报警拓扑


===================================【配置变更记录】==================================

2019-02-15
1.ants_1.0_beta版本提交.
