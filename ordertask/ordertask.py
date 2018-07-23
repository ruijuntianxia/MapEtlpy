import pyodbc
import cx_Oracle
import redis
import json
import collections
import filelog as log
import Redislist as rl

try: 
    #订单任务
    rl.getliststr("ordertask",'view')
    #任务单准时送货率
    rl.getPod("orderPod","select sum(countnum) countnum,sum(countnumh) countnumh,100*round( sum(countnumh)/SUM(sum(countnum)) OVER(),4)||'%' per  from v_ordertask ")
    #订单图表出货
    rl.getlist("orderdelivefrom")
    #订单图表送达
    rl.getlist("orderdeliveto")
    
    
    #数据表转换成数值组
    #rl.getliststr("order_list","table")

except Exception as result:
    print(result)
