from TransittrackOther import TransitKhg,TransitZj

import cx_Oracle

conn = cx_Oracle.connect('SCMP/ZhwlScMp2018@120.77.205.81:1521/orcl')   
curs=conn.cursor()
msg = curs.var(cx_Oracle.CURSOR) #plsql出参
#调用存储过程 存在轨迹订单查询
curs.callproc('PACK_OP.PRO_OrderlistShip', [msg])

data=msg.getvalue().fetchall()
print(type(data))
for ro in data:
    if ro[2]=='khg':
        
        '''
        看货狗轨迹数据
        第一个参数业务ID
        ro[0]订单编号
        '''
        TransitKhg.ShipMaplist('',ro[0],conn)
    if ro[2]=='zj':
        
        '''
        中交轨迹数据
        第一个参数业务ID
        ro[0]订单编号
        '''
        TransitZj.ShipMaplistZJ('',ro[0],conn)

conn.close()


