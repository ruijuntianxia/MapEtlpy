import pyodbc
import cx_Oracle
import redis
import json
import collections
import filelog as log

try:

    '''
    @此为订单对应运输轨迹详情
    '''


    #makedsn(IP/HOST,PORT,TNSNAME)
    #dsn=cx_Oracle.makedsn('120.77.205.81','1521','orcl')
    #conn=cx_Oracle.connect('SCMP','ZhwlScMp2018',dsn)
    #用自己的实际数据库用户名、密码、主机ip地址 替换即可
    #conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    print("访问地址：SCMP/ZhwlScMp2018@172.18.218.224:1521/orcl")
    conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    #print(conn)
    curs=conn.cursor()

    #声明变量
    shipno='0000306559'
    pagestar =1
    pageend =4
    typename = 'DX3Q03' #plsql入参
    msg = curs.var(cx_Oracle.CURSOR) #plsql出参
    cunm = curs.var(cx_Oracle.CURSOR) #plsql出参
    #调用存储过程
    curs.callproc('PACK_OP.pro_shipmapliso', [shipno,pagestar,pageend, msg,cunm]) #['Nick', 'Nick, Good Morning!']
    #nds=msg.getvalue().fetchall()
    #rows=curs.fetchone()单个元组
    #游标先获取值在fetcall 
    rows=msg.getvalue().fetchall()
    countrows=cunm.getvalue().fetchall()
    curs.close()
    conn.close()
     #正式
    #r = redis.Redis(host='127.0.0.1', port=6379,db=0)
    #测试
    r = redis.Redis(host='120.77.205.81', port=6379,db=0,password='zh123')
    
    r.set('shipmentcount',countrows)
    message="insert:"+str(countrows)
    log.logCommon(r,"shipment",message)
    r.ltrim("hashname10",1,0)

    # 手动整理坐标点汇总
    objects_list = [] 
    numid=0
    for ro in rows:
        # 已经有数据
        if  len(objects_list)>0:
            # 是否是同一个运单
            if objects_list[0]==ro[0]:
                objects_list[6]+=','+str(ro[6])
                #print(objects_list[6])
            else:
                # 不是一个运单则插入redis，同时重新给objects_list赋值
                numid+=1
                print(str(objects_list[0]))
                dic={"TMS_SHIPMENT_NO":str(objects_list[0]),\
                "TMS_PLATE_NUMBER" : str(objects_list[1]) ,\
                "DOGNUMBER" : str(objects_list[2]),\
                "NAME" :str(objects_list[3]) ,\
                "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
                "OTS_RETURN_DATE" :str(objects_list[5]).encode('utf8').decode('utf-8'),\
                "PATH" : '['+str(objects_list[6])+']'  }
                r.lpush('hashname10',dic)  
                message=str(numid)
                log.logCommon(r,"insertship",message)
                print(r.llen("hashname10")) 
                objects_list[0]=ro[0]
                objects_list[1]=ro[1]
                objects_list[2]=ro[2]
                objects_list[3]=ro[3]
                objects_list[4]=ro[4]
                objects_list[5]=ro[5]
                objects_list[6]=ro[6]

        else:
            #第一次进入
            objects_list.append(ro[0])
            objects_list.append(ro[1])
            objects_list.append(ro[2])
            objects_list.append(ro[3])
            objects_list.append(ro[4])
            objects_list.append(ro[5])
            objects_list.append(ro[6])
    numid+=1
    print(objects_list[0])
    dic={"TMS_SHIPMENT_NO":objects_list[0],\
                "TMS_PLATE_NUMBER" : objects_list[1] ,\
                "DOGNUMBER" : objects_list[2],\
                "NAME" :objects_list[3] ,\
                "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
                "OTS_RETURN_DATE" :str(objects_list[5]).encode('utf8').decode('utf-8'),\
                "PATH" : '['+objects_list[6]+']'  }
    r.lpush('hashname10',dic)  
    message=str(numid)
    log.logCommon(r,"insertship",message)
    print(r.llen("hashname10"))
    #数据库地理坐标去重之后如果数据量不大 可以在一个栏位 不超出栏位最大长度存储直接使用


except Exception as result:
    print(result)
