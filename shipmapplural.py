import pyodbc
import cx_Oracle
import redis
import json
import collections
import filelog as log

try:
#makedsn(IP/HOST,PORT,TNSNAME)
#dsn=cx_Oracle.makedsn('120.77.205.81','1521','orcl')
#conn=cx_Oracle.connect('SCMP','ZhwlScMp2018',dsn)
#用自己的实际数据库用户名、密码、主机ip地址 替换即可
#conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    print("访问地址：SCMP/ZhwlScMp2018@172.18.218.224:1521/orcl")
    conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    #print(conn)
    curs=conn.cursor()
    #sql='select a.* from scmp.V_tptshipment a' #sql语句 坐标点信息提前汇总，如果坐标点不是很多可以使用  
    sql='select a.* from scmp.v_tptshipmentu a' #sql语句 坐标点过多，oracle一个栏位无法存储，在转档时进行存储 
    rr=curs.execute (sql)
    #rows=curs.fetchone()单个元组
    rows=curs.fetchall()
    #print(rows[0])
    sqlCount='select a.* from  scmp.v_shipmentcount a'
    countr=curs.execute(sqlCount)
    countrows=curs.fetchone()

     
    r = redis.Redis(host='127.0.0.1', port=6379,db=0)
    r.set('shipmentcount',countrows[0])
    message="insert:"+str(countrows[0])
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
               
                objects_list[6]+=','+ro[5]
                #print(objects_list[6])
            else:
                # 不是一个运单则插入redis，同时重新给objects_list赋值
                numid+=1
                print(objects_list[0])
                dic={"TMS_SHIPMENT_NO":objects_list[0],\
                "TMS_PLATE_NUMBER" : objects_list[1] ,\
                "DOGNUMBER" : objects_list[2],\
                "NAME" :objects_list[3] ,\
                "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
                "OTS_RETURN_DATE" :objects_list[5],\
                "PATH" : '['+objects_list[6]+']'  }
                r.lpush('hashname10',dic)  
                message=str(numid)
                log.logCommon(r,"insertship",message)
                print(r.llen("hashname10")) 
                objects_list[0]=ro[0]
                objects_list[1]=ro[1]
                objects_list[2]=ro[2]
                objects_list[3]=ro[3]
                objects_list[4]=ro[4]
                objects_list[5]=""
                objects_list[6]=ro[5]

        else:
            #第一次进入
            objects_list.append(ro[0])
            objects_list.append(ro[1])
            objects_list.append(ro[2])
            objects_list.append(ro[3])
            objects_list.append(ro[4])
            objects_list.append("")
            objects_list.append(ro[5])
    numid+=1
    print(objects_list[0])
    dic={"TMS_SHIPMENT_NO":objects_list[0],\
                "TMS_PLATE_NUMBER" : objects_list[1] ,\
                "DOGNUMBER" : objects_list[2],\
                "NAME" :objects_list[3] ,\
                "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
                "OTS_RETURN_DATE" :objects_list[5],\
                "PATH" : '['+objects_list[6]+']'  }
    r.lpush('hashname10',dic)  
    message=str(numid)
    log.logCommon(r,"insertship",message)
    print(r.llen("hashname10"))
    #数据库地理坐标去重之后如果数据量不大 可以在一个栏位 不超出栏位最大长度存储直接使用
    #r.ltrim("hashname10",1,0)
    #numid=0
    #for row in rows:
        #numid+=1
        #print(row[5])
        #dic={"TMS_SHIPMENT_NO":row[0],"TMS_PLATE_NUMBER" : row[1] ,"DOGNUMBER" : row[2],"NAME" :row[3] ,"STARTDATE" : str(row[4]).encode('utf8').decode('utf-8'),"OTS_RETURN_DATE" :"","PATH" : row[5]  }
        #r.lpush('hashname10',dic)  
        #message=str(numid)
        #log.logCommon(r,"insertship",message)
    #print(r.llen("hashname10"))
    
    curs.close()
    conn.close() 

except Exception as result:
    print(result)
