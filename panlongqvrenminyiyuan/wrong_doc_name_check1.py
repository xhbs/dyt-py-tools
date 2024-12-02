from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

# 创建第一个引擎连接到第一个 schema
# dyt_order_center = create_engine(
#     'mysql+mysqlconnector://dyt:dyt@rm-2ze67irop6b6vsn9zno.mysql.rds.aliyuncs.com/dyt_appointment_order')
# dyt_tenant = create_engine('mysql+mysqlconnector://dyt:dyt@rm-2ze67irop6b6vsn9zno.mysql.rds.aliyuncs.com/dyt_tenant')


# 创建数据库连接引擎
engine = create_engine('mysql+mysqlconnector://dyt:dyt@rm-2ze67irop6b6vsn9zno.mysql.rds.aliyuncs.com')

# 创建会话
Session = sessionmaker(bind=engine)
session = Session()

# 定义元数据
metadata = MetaData()

# 指定表格和schema
order_table = Table('t_order', metadata, schema='dyt_appointment_order')
doctor_table = Table('t_doctor', metadata, schema='dyt_tenant')

# 第一个查询
order_query = order_table.select().where(
    order_table.c.hospital_code.in_(['871667', '871167', '871567', '871467', '871367', '871267']),
    order_table.c.create_time > '2024-04-18 00:00:00'
)

# 执行第一个查询
order_results = session.execute(order_query)

# 获取doctor_code集合
doctor_codes = [row['doctor_code'] for row in order_results]

# 第二个查询
doctor_query = doctor_table.select().where(doctor_table.c.doctor_code.in_(doctor_codes))

# 执行第二个查询
doctor_results = session.execute(doctor_query)

# 处理结果
for row in doctor_results:
    print(row)

# 关闭会话
session.close()
