from sqlalchemy import create_engine, Column, Integer, String, DateTime, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 定义Base
Base = declarative_base()


# 定义TOrder模型
class Order(Base):
    __tablename__ = 't_order'
    __table_args__ = {'schema': 'order_center'}
    id = Column(Integer, primary_key=True)
    hospital_code = Column(String)
    create_time = Column(DateTime)
    doctor_code = Column(String)  # 假设t_order表中包含医生代码以便直接关联查询


# 定义TDoctor模型（虽然第二部分查询未直接使用，但保持完整性）
class Doctor(Base):
    __tablename__ = 't_doctor'
    __table_args__ = {'schema': 'tenant'}
    id = Column(Integer, primary_key=True)
    name = Column(String)
    thrdpart_doctor_code = Column(String)


# 配置数据库引擎
engine_order_center = create_engine(
    'mysql+mysqlconnector://dyt:dyt@rm-2ze67irop6b6vsn9zno.mysql.rds.aliyuncs.com/dyt_appointment_order')
engine_tenant = create_engine('mysql+mysqlconnector://dyt:dyt@rm-2ze67irop6b6vsn9zno.mysql.rds.aliyuncs.com/dyt_tenant')

# 创建会话工厂
SessionOrderCenter = sessionmaker(bind=engine_order_center)
SessionTenant = sessionmaker(bind=engine_tenant)  # 同上，可能不需要

# 开启会话
session_oc = SessionOrderCenter()

# 第一部分查询
order_query = session_oc.query(Order).filter(
    Order.hospital_code.in_(('871667', '871167', '871567', '871467', '871367', '871267')),
    Order.create_time > '2024-04-18 00:00:00'
)
results_order = order_query.all()

# 由于第二个查询实际上依赖于第一个查询的结果，且我们假设t_order表中包含了所需信息，
# 因此直接使用第一个查询的结果，无需进行第二次数据库查询。如果t_doctor表中包含额外信息且必须查询，
# 则需根据实际需求调整逻辑。

# 打印或进一步处理查询结果
for result in results_order:
    print(result.id, result.hospital_code, result.create_time, result.doctor_code)
