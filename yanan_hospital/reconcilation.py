from datetime import datetime
from typing import TypeVar, Union

import mysql.connector
import pandas as pd

from tool.CommonUtils import is_right_order_no
from tool.ReconciliationTools import ReconciliationTools

IntStrT = TypeVar("IntStrT", bound=Union[int, str])

base_dir = 'D:\\DYT - JobFile\\延安医院\\对账\\2024-07\\01~11\\'
his_records_file_name = 'tmp001.xlsx'
wx_records_file_name = '1414216802All2024-07-01_2024-07-11.csv'
t_order_file_name = 'refactor_t_order.xlsx'
recon_date = datetime(2024, 7, 11)

tool = ReconciliationTools(base_dir, wx_records_file_name, recon_date)


def execute():
    his_records = get_his_records()
    wx_records = tool.get_wx_records(remove_wx_zero=True)
    tool.wx_verify_his(wx_records, his_records)


def get_his_records(sheet_name: str | int | list[IntStrT] | None = 0,
                    order_no_column: str = 'PLATFORM_NO',
                    date_column: str = 'THIRD_TRADE_TIME',
                    fee_column: str = 'TRADE_MONEY', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, sheet_name=sheet_name)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict[order_no_column]):
        if not is_right_order_no(his_records_data_dict[order_no_column][index]):
            index += 1
            continue
        ope_date = datetime.combine(his_records_data_dict[date_column][index].to_pydatetime().date(),
                                    datetime.min.time())
        if ope_date != recon_date:
            index += 1
            continue
        his_records_dict.update(
            {his_records_data_dict[order_no_column][index]: his_records_data_dict[fee_column][index]})
        index += 1
    return his_records_dict


# def get_t_order_data():
#     if len(t_order_dict) != 0:
#         return t_order_dict
#     t_order_pd = pd.read_excel(base_dir + t_order_file_name)
#     t_order_excel_dict = t_order_pd.to_dict()
#     index = 0
#     while index < len(t_order_excel_dict['id']):
#         order = TOrder(t_order_excel_dict['id'][index], t_order_excel_dict['order_no'][index],
#                        t_order_excel_dict['jz_card'][index], t_order_excel_dict['patient_name'][index],
#                        t_order_excel_dict['create_time'][index], t_order_excel_dict['update_time'][index],
#                        t_order_excel_dict['cancel_time'][index])
#         if order.order_no not in t_order_dict:
#             order_set_temp = set[TOrder]()
#         else:
#             order_set_temp = t_order_dict[order.order_no]
#         order_set_temp.add(order)
#         t_order_dict.update({order.order_no: order_set_temp})
#
#         index += 1
#     return t_order_dict


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


def search_order(sheet_name: str | int | list[IntStrT] | None = 0,
                 order_no_column: str = 'PLATFORM_NO',
                 date_column: str = 'THIRD_TRADE_TIME',
                 fee_column: str = 'TRADE_MONEY', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, sheet_name=sheet_name)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    jz_card_list = list[str]()
    while index < len(his_records_data_dict[order_no_column]):
        ope_date = datetime.combine(his_records_data_dict[date_column][index].to_pydatetime().date(),
                                    datetime.min.time())
        if ope_date != recon_date:
            index += 1
            continue
        if his_records_data_dict[fee_column][index] < 0:
            jz_card_list.append(str(his_records_data_dict['PATIENT_ID'][index]))
        index += 1
    cnx = mysql.connector.connect(**mysql_config_refactor)
    cursor = cnx.cursor()
    order_no_list = list[str]()
    for card in jz_card_list:
        # 执行查询语句
        query = f"SELECT order_no FROM t_order where jz_card like '%{card}%' and hospital_code = '871093' and DATE(cancel_time) = '{recon_date.date()}'"
        cursor.execute(query)
        # 获取查询结果
        results = cursor.fetchall()
        for result in results:
            # 假设result是一个包含字段值的元组，例如(result[0], result[1], ...)
            field_value = result[0]  # 选择你需要的字段
            order_no_list.append(field_value)
    # 关闭连接
    cursor.close()
    cnx.close()

    cnx = mysql.connector.connect(**mysql_config_dyt_kf)
    cursor = cnx.cursor()
    # for card in jz_card_list:
    # 执行查询语句
    jz_card_str = ",".join(jz_card_list)
    query = f"SELECT order_no FROM api_appoint_record where jz_card in ({jz_card_str}) and hos_id = '871093' and DATE(FROM_UNIXTIME(cancel_time)) = '{recon_date.date()}'"
    cursor.execute(query)
    # 获取查询结果
    results = cursor.fetchall()
    for result in results:
        # 假设result是一个包含字段值的元组，例如(result[0], result[1], ...)
        field_value = result[0]  # 选择你需要的字段
        order_no_list.append(field_value)
    # 关闭连接
    cursor.close()
    cnx.close()
    return order_no_list


def get_wx_refund_record():
    wx_records = tool.get_wx_records(remove_wx_zero=True)
    sum_refund = 0
    for wx_record in wx_records:
        if wx_records[wx_record] < 0:
            sum_refund += wx_records[wx_record]
    print(sum_refund)


def get_his_refund_record(sheet_name: str | int | list[IntStrT] | None = 0,
                          order_no_column: str = 'PLATFORM_NO',
                          date_column: str = 'THIRD_TRADE_TIME',
                          fee_column: str = 'TRADE_MONEY', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, sheet_name=sheet_name)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict['TRADE_MONEY']):
        ope_date = datetime.combine(his_records_data_dict[date_column][index].to_pydatetime().date(),
                                    datetime.min.time())
        if ope_date != recon_date:
            index += 1
            continue
        if his_records_data_dict[fee_column][index] < 0:
            his_records_dict.update(
                {his_records_data_dict['   '][index]: his_records_data_dict[fee_column][index]})
        index += 1

    sum_refund = 0
    for his_record in his_records_dict:
        if his_records_dict[his_record] < 0:
            sum_refund += his_records_dict[his_record]
    print(sum_refund)


def find_wx_more_refund_order():
    his_refund_order = search_order()
    wx_records = tool.get_wx_records(remove_wx_zero=True)
    for wx_record in wx_records:
        if wx_records[wx_record] < 0:
            if wx_record not in his_refund_order:
                print(wx_record)


if __name__ == '__main__':
    find_wx_more_refund_order()
