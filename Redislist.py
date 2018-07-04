import pyodbc
import cx_Oracle
import redis
import json
import collections
import filelog as log


#简单list 组成数组 二维数组
def getlist(name):
    conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    curs=conn.cursor()
    sql='select a.* from scmp.v_'+name+' a' #sql语句 坐标点过多，oracle一个栏位无法存储，在转档时进行存储 
    rr=curs.execute (sql)
    rows=curs.fetchall()
    r = redis.Redis(host='127.0.0.1', port=6379,db=0)
    r.ltrim(name,1,0)
    # 手动整理坐标点汇总
    objects_list = [] 
    numid=0
    for ro in rows:
        # 已经有数据
        if  len(objects_list)>0:
            # 是否是同一个运单
            objects_list[0]+=','+ro[0]
            print(objects_list[0])
        else:
            #第一次进入
            objects_list.append(ro[0])
    dic=objects_list[0]
    r.lpush(name,dic)  
    message=str(numid)
    log.logCommon(r,name,message)
    print(r.llen(name)) 
    curs.close()
    conn.close() 

# datatable转json 字符串键值对
def getliststr(name,tableView):
    conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    curs=conn.cursor()

    if tableView =="view":
        sql='select rownum,a.* from scmp.v_'+name+' a ' #sql语句 坐标点过多，oracle一个栏位无法存储，在转档时进行存储 
    else:
        sql='select a.* from scmp.'+name+' a'
    rr=curs.execute (sql)
    rows=curs.fetchall()
    columnname=rr.description
    r = redis.Redis(host='127.0.0.1', port=6379,db=0)
    r.ltrim(name,1,0)
    # 手动整理坐标点汇总
    
    numid=0
    for ro in rows:
        row={}
        objects_list = []
        for i in range(len(columnname)):
            # 已经有数据
            row[columnname[i][0]]=ro[i]
        objects_list.append(row)
        dic=objects_list[0]
        #rpush 右新增，按照插入数据顺序显示数据
        #lpush 做新增，按照插入数据倒序显示数据，因左增导致先进入的数据排序在后
        r.rpush(name,dic)  
        message=str(numid)
        log.logCommon(r,name,message)

    print(r.llen(name)) 
    curs.close()
    conn.close() 
    print(objects_list)

# datatable转json 字符串键值对
def getPod(name,sql):
    conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
    curs=conn.cursor()
    rr=curs.execute (sql)
    rows=curs.fetchall()
    columnname=rr.description
    r = redis.Redis(host='127.0.0.1', port=6379,db=0)
    r.ltrim(name,1,0)
    # 手动整理坐标点汇总
    
    numid=0
    for ro in rows:
        row={}
        objects_list = []
        for i in range(len(columnname)):
            # 已经有数据
            row[columnname[i][0]]=ro[i]
        objects_list.append(row)
        dic=objects_list[0]
        r.lpush(name,dic)  
        message=str(numid)
        log.logCommon(r,name,message)

    print(r.llen(name)) 
    curs.close()
    conn.close() 
    print(objects_list)