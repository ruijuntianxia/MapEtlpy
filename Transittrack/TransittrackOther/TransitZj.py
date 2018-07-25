#__file__获取当前程序的相对路径
import os,sys
#print(__file__)
# os.path.abspath(__file__) 获取当前程序的绝对路径
print(os.path.abspath(__file__))
# E:\my python study\day4\Atm\bin\atm.py
Path=os.path.abspath(__file__)
# print(os.path.dirname(Path))获取当前程序的父目录的绝对路径
BASE_DIR=os.path.dirname(os.path.dirname(Path))
print(BASE_DIR)
# E:\my python study\day4\Atm
#将BASE_DIR添加到系统环境变量中
sys.path.append(BASE_DIR)

from Log import filelog as log

import pyodbc
import cx_Oracle
import redis
import json
import collections

def ShipMaplistZJ(typename,orderno,conn):
    try:
        '''
        @此为订单对应运输轨迹详情 对应轨迹地图
        '''
        #makedsn(IP/HOST,PORT,TNSNAME)
        #dsn=cx_Oracle.makedsn('120.77.205.81','1521','orcl')
        #conn=cx_Oracle.connect('SCMP','ZhwlScMp2018',dsn)
        #用自己的实际数据库用户名、密码、主机ip地址 替换即可
        conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
        #conn = cx_Oracle.connect('SCMP/scmp123456@192.168.1.193:1521/orcl')   

        
        #conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
        
        curs=conn.cursor()
        

        #声明变量
        #typename = ''#320' #plsql入参
        msg = curs.var(cx_Oracle.CURSOR) #plsql出参
        cunm = curs.var(cx_Oracle.CURSOR) #plsql出参
        #调用存储过程
        curs.callproc('PACK_OP.PRO_ShipMapLisZJ', [typename,orderno, msg,cunm]) #['Nick', 'Nick, Good Morning!']
        #nds=msg.getvalue().fetchall()
        #rows=curs.fetchone()单个元组
        #游标先获取值在fetcall 
        rows=msg.getvalue().fetchall()
        if len(rows)==0:
            print(orderno+":ZJ订单没有轨迹")
            return
        print(type(rows))
        curs.close()
        conn.close()


        #正式
        r = redis.Redis(host='120.77.205.81', port=6379,db=6,password='zh123')
        #测试
        #r = redis.Redis(host='127.0.0.1', port=6379,db=6,password='zh123')
        countn=r.get('TransittrackOnly')
        countnum=0
        if countn!=None:
            countnum=int(countn)
        
        # 手动整理坐标点汇总
        objects_list = [] 
        count=0
        for ro in rows:
            
            # 已经有数据
            if  len(objects_list)>0:
                # 是否是同一个运单
                if str(ro[0])=="None" or str(objects_list[0])=="None" :
                    continue
                if objects_list[0]==ro[0] and objects_list[2]==ro[2]:
                    count+=1
                    objects_list[6]+=','+'{ \'name\':\''+str(ro[8])+'【'+str(ro[9]).encode('utf8').decode('utf-8')+'】\',\'lnglat\':'+str(ro[7])+'}'
                    #print(objects_list[6])
                else:
                    # 不是一个运单则插入redis，同时重新给objects_list赋值
                    
                    keyname=str(objects_list[0])+ str(objects_list[2])
                    keyvalue=r.lrange(keyname,0,1)
                    if len(keyvalue)==1:
                        if len(eval(keyvalue[0])["path"])==(len(objects_list[6])+2):
                            print("已存在")
                            pass
                        else:
                            countnum+=1
                            objects_list[6]=str("["+objects_list[6]+"]")
                            r.ltrim(keyname,1,0)
                            dic={"TMS_SHIPMENT_NO":str(objects_list[0]),\
                            "TMS_PLATE_NUMBER" : str(objects_list[1]) ,\
                            "DOGNUMBER" : str(objects_list[2]),\
                            "name" :str(objects_list[3]) ,\
                            "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
                            "OTS_RETURN_DATE" :str(objects_list[5]).encode('utf8').decode('utf-8'),\
                            "path" :objects_list[6],\
                            "status":'zj',\
                        "count": count }

                            r.lpush(keyname,dic)  
                            #message=str(numid)
                            #log.logCommon(r,"insertship",message)
                            print(keyname) 
                    else:
                        countnum+=1
                        objects_list[6]=str("["+objects_list[6]+"]")
                        r.ltrim(keyname,1,0)
                        dic={"TMS_SHIPMENT_NO":str(objects_list[0]),\
                        "TMS_PLATE_NUMBER" : str(objects_list[1]) ,\
                        "DOGNUMBER" : str(objects_list[2]),\
                        "name" :str(objects_list[3]) ,\
                        "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
                        "OTS_RETURN_DATE" :str(objects_list[5]).encode('utf8').decode('utf-8'),\
                        "path" :objects_list[6],\
                        "status":'zj',\
                        "count": count }

                        r.lpush(keyname,dic)  
                        #message=str(numid)
                        #log.logCommon(r,"insertship",message)
                        print(keyname) 
                    objects_list=[]

            else:
                #第一次进入
                count=1
                objects_list.append(ro[0])
                objects_list.append(ro[1])
                objects_list.append(ro[2])
                objects_list.append(ro[4])
                objects_list.append(ro[5])
                objects_list.append(ro[6])
                objects_list.append('{ \'name\':\''+str(ro[8])+'【'+str(ro[9]).encode('utf8').decode('utf-8')+'】\',\'lnglat\':'+str(ro[7])+'}')
        
        keyname=str(ro[0])+ str(ro[2])
        keyvalue=r.lrange(keyname,0,1)
        if len(keyvalue)>0:
            print("已存在")
            pass
        else:
            
            countnum+=1
            keyname=str(objects_list[0])+ str(objects_list[2])
            r.ltrim(keyname,1,0)
            dic={"TMS_SHIPMENT_NO":str(objects_list[0]),\
            "TMS_PLATE_NUMBER" : str(objects_list[1]) ,\
            "DOGNUMBER" : str(objects_list[2]),\
            "NAME" :str(objects_list[3]) ,\
            "STARTDATE" : str(objects_list[4]).encode('utf8').decode('utf-8'),\
            "OTS_RETURN_DATE" :str(objects_list[5]).encode('utf8').decode('utf-8'),\
            "PATH" : '['+str(objects_list[6])+']',\
            "status":'zj',\
            "count": count  }
            r.lpush(keyname,dic)  
            #message=str(numid)
            #log.logCommon(r,"insertship",message)
            print(keyname)
            
        
        r.set('TransittrackOnly',countnum)
        message="insert:"+str(countnum)
        log.logCommon(r,"shipment",message)

    except Exception as result:
        print(result)

