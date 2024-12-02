from datetime import datetime
from typing import TypeVar, Union

import mysql.connector
import pandas as pd

from tool.ReconciliationTools import ReconciliationTools

IntStrT = TypeVar("IntStrT", bound=Union[int, str])

base_dir = 'D:\\DYT - JobFile\\省中西医结合\\6月\\'
his_records_file_name = '滇医通6月份挂号信息.xlsx'
wx_records_file_name = '1651179416All2024-06-01_2024-06-30.csv'
recon_date = datetime(2024, 6, 1)

tool = ReconciliationTools(base_dir, wx_records_file_name, recon_date)


def execute():
    his_records = get_his_records_gb_patient()
    wx_records = wx_record_group_by_patient(tool.get_wx_records(remove_wx_zero=True))
    print("over")
    # tool.wx_verify_his(wx_records, his_records)


def get_his_records(sheet_name: str | int | list[IntStrT] | None = 0,
                    order_no_column: str = '聚合支付返回订单号',
                    date_column: str = '下单时间P',
                    fee_column: str = '金额', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, sheet_name=sheet_name)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict[order_no_column]):
        his_records_dict.update(
            {his_records_data_dict[order_no_column][index]: his_records_data_dict[fee_column][index]})
        index += 1
    return his_records_dict


def get_his_records_gb_patient(sheet_name: str | int | list[IntStrT] | None = 0,
                               order_no_column: str = '聚合支付返回订单号',
                               date_column: str = '下单时间P',
                               fee_column: str = '金额', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, sheet_name=sheet_name)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict[order_no_column]):
        ope_date = datetime.combine(datetime.strptime(his_records_data_dict[date_column][index], "%Y-%m-%d %H:%M:%S"),
                                    datetime.min.time())
        if ope_date != recon_date:
            index += 1
            continue
        jz_card = his_records_data_dict['就诊卡号'][index]
        order_dict = {'jz_card': jz_card, 'total_fee': his_records_data_dict[fee_column][index]}
        if jz_card not in his_records_dict:
            order_array = []
        else:
            order_array = his_records_dict[jz_card]
        order_array.append(order_dict)
        his_records_dict[jz_card] = order_array
        index += 1
    return his_records_dict


def get_wx_records(tool: ReconciliationTools, remove_wx_zero=False) -> dict:
    his_records_esc_data = pd.read_csv(tool.base_dir + tool.wx_records_file_name, skipfooter=2, engine='python')
    his_records_data_dict = his_records_esc_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict['商户订单号']):
        order_no = str(his_records_data_dict['商户订单号'][index]).replace('`', '')
        if his_records_data_dict['交易状态'][index] == '`SUCCESS':
            fee = float(str(his_records_data_dict['应结订单金额'][index]).replace('`', ''))
        else:
            fee = float(str(his_records_data_dict['退款金额'][index]).replace('`', '')) * -1
        if order_no in his_records_dict:
            order_no = his_records_data_dict['商户退款单号'][index]
        if remove_wx_zero and fee == 0:
            his_records_dict.pop(order_no)
        else:
            his_records_dict.update({order_no: fee})
        index += 1
    return his_records_dict


# 生产MySQL数据库连接配置
mysql_config_refactor = {
    'host': 'rm-2ze67irop6b6vsn9zno.mysql.rds.aliyuncs.com',
    'user': 'refactor_biz_user',
    'password': 'JaEo10iN0t5eOA1w',
    'database': 'dyt_appointment_order'
}
mysql_config_dyt_kf = {
    'host': 'rm-2ze12q56u47150n2co.mysql.rds.aliyuncs.com',
    'user': 'hdkcdbuser',
    'password': 'Dyt123456',
    'database': 'dytkf'
}


def wx_record_group_by_patient(wx_records: dict[str, any]):
    wx_records_gb_patient = dict[str, list[dict]]()
    if not wx_records and len(wx_records) == 0:
        return wx_records_gb_patient
    cnx = mysql.connector.connect(**mysql_config_refactor)
    cursor = cnx.cursor()
    order_no_str = ",".join(map(lambda x: f"'{x}'", wx_records.keys()))
    # 执行查询语句
    query = f"SELECT `order_no`,`jz_card`,`total_fee` FROM t_order where `order_no` in ({order_no_str}) and hospital_code = '871094'"
    cursor.execute(query)
    # 获取查询结果
    results = cursor.fetchall()
    wx_records_gb_patient = build_wx_order_dict(results, wx_records_gb_patient)
    # 关闭连接
    cursor.close()
    cnx.close()

    cnx = mysql.connector.connect(**mysql_config_dyt_kf)
    cursor = cnx.cursor()
    # for card in jz_card_list:
    # 执行查询语句
    query = f"SELECT `order_no`,`jz_card`,`amt` as total_fee FROM api_appoint_record where `order_no` in ({order_no_str}) and hos_id = '871094'"
    cursor.execute(query)
    # 获取查询结果
    results = cursor.fetchall()
    wx_records_gb_patient = build_wx_order_dict(results, wx_records_gb_patient)
    # 关闭连接
    cursor.close()
    cnx.close()

    return wx_records_gb_patient


def build_wx_order_dict(results: list, wx_records_gb_patient: dict[str, list[dict]]()) -> dict:
    for result in results:
        order_no, jz_card, total_fee = result
        order_dict = {'order_no': order_no, 'jz_card': jz_card, 'total_fee': total_fee}
        if jz_card not in wx_records_gb_patient:
            order_array = []
        else:
            order_array = wx_records_gb_patient[jz_card]
        order_array.append(order_dict)
        wx_records_gb_patient[jz_card] = order_array
    return wx_records_gb_patient


def sum_pay_fee():
    his_records = get_his_records()
    sum_fee = 0
    count = 0
    for record in his_records:
        if his_records[record] > 0:
            sum_fee += his_records[record]
            count += 1
    print('HIS总应收金额：' + str(sum_fee))
    print('HIS总应收笔数：' + str(count))
    wx_records = get_wx_records(tool, remove_wx_zero=True)
    sum_fee = 0
    count = 0
    for record in wx_records:
        if wx_records[record] > 0:
            sum_fee += wx_records[record]
            count += 1
    print('微信总收取金额：' + str(sum_fee))
    print('微信总收取笔数：' + str(count))


def sum_refund_fee():
    his_records = get_his_records()
    sum_fee = 0
    count = 0
    for record in his_records:
        if his_records[record] < 0:
            sum_fee += his_records[record]
            count += 1
    print('HIS总应退金额：' + str(sum_fee))
    print('HIS总应退笔数：' + str(count))
    wx_records = get_wx_records(tool, remove_wx_zero=True)
    sum_fee = 0
    count = 0
    for record in wx_records:
        if wx_records[record] < 0:
            sum_fee += wx_records[record]
            count += 1
    print('微信总退款金额：' + str(sum_fee))
    print('微信总退款笔数：' + str(count))


if __name__ == '__main__':
    sum_pay_fee()
    print('*******************')
    sum_refund_fee()
