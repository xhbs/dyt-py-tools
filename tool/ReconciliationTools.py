import os
from datetime import datetime

import pandas as pd


class ReconciliationTools:
    base_dir: str
    wx_records_file_name: str
    recon_date: datetime
    result_parent_dir: str

    def __init__(self, base_dir, wx_records_file_name, recon_date, result_parent_dir='result'):
        self.base_dir = base_dir
        self.wx_records_file_name = wx_records_file_name
        self.recon_date = recon_date
        self.result_parent_dir = result_parent_dir

    def get_wx_records(self, remove_wx_zero=False) -> dict:
        his_records_esc_data = pd.read_csv(self.base_dir + self.wx_records_file_name, skipfooter=2, engine='python')
        his_records_data_dict = his_records_esc_data.to_dict()
        index = 0
        his_records_dict = dict()
        while index < len(his_records_data_dict['商户订单号']):
            if not '预约挂号' in str(his_records_data_dict['商品名称'][index]):
                index += 1
                continue
            record_date = datetime.strptime(his_records_data_dict['交易时间'][index], '`%Y-%m-%d %H:%M:%S').date()
            if record_date == self.recon_date.date():
                order_no = str(his_records_data_dict['商户订单号'][index]).replace('`', '')
                if his_records_data_dict['交易状态'][index] == '`SUCCESS':
                    fee = float(str(his_records_data_dict['应结订单金额'][index]).replace('`', ''))
                else:
                    fee = float(str(his_records_data_dict['退款金额'][index]).replace('`', '')) * -1
                if order_no in his_records_dict:
                    fee += his_records_dict[order_no]
                if remove_wx_zero and fee == 0:
                    his_records_dict.pop(order_no)
                else:
                    his_records_dict.update({order_no: fee})
            index += 1
        return his_records_dict

    def wx_verify_his(self, wx_records, his_records):
        print("以微信账单为基准匹配his账单：")
        unilateral_wx2his = dict()
        for order_no in wx_records:
            if order_no in his_records:
                if wx_records[order_no] != his_records[order_no]:
                    print('订单号：' + order_no + ' 金额：' + str(wx_records[order_no]) + ','
                          + str(his_records[order_no]) + ' 匹配失败')
            else:
                print('订单号：' + order_no + ' 金额：' + str(wx_records[order_no]) + ' 匹配失败')
                unilateral_wx2his.update({order_no: wx_records[order_no]})
        if len(unilateral_wx2his) != 0:
            df = pd.DataFrame.from_dict(unilateral_wx2his, orient='index', columns=['金额'])
            self.to_disk(df, self.recon_date.strftime('%#m月%#d日') + '微信核his匹配失败订单.xlsx')

    def his_verify_wx(self, his_records, wx_records):
        print("以his账单为基准匹配微信账单：")
        unilateral_his2wx = dict()
        for order_no in his_records:
            if order_no in wx_records:
                if his_records[order_no] != wx_records[order_no]:
                    print('订单号：' + order_no + ' 金额：' + str(his_records[order_no]) + ','
                          + str(wx_records[order_no]) + ' 匹配失败')
            else:
                print('订单号：' + order_no + ' 金额：' + str(his_records[order_no]))
                unilateral_his2wx.update({order_no: his_records[order_no]})
        if len(unilateral_his2wx) != 0:
            df = pd.DataFrame.from_dict(unilateral_his2wx, orient='index', columns=['金额'])
            self.to_disk(df, self.recon_date.strftime('%#m月%#d日') + 'his应收/退匹配微信失败订单.xlsx')

    def to_disk(self, df: pd.DataFrame, file_name: str):
        df.index.name = '订单号'
        result_path = self.base_dir + self.result_parent_dir + "\\"
        if not os.path.exists(result_path):
            os.makedirs(result_path)
        df.to_excel(result_path + file_name)


def sum_pay_fee(self, his_records: dict):
    sum_fee = 0
    count = 0
    for record in his_records:
        if his_records[record] > 0:
            sum_fee += his_records[record]
            count += 1
    print(sum_fee)
    print(count)
    wx_records = self.get_wx_records(remove_wx_zero=True)
    sum_fee = 0
    count = 0
    for record in wx_records:
        if wx_records[record] > 0:
            sum_fee += wx_records[record]
            count += 1
    print(sum_fee)
    print(count)


def sum_refund_fee(self, his_records: dict):
    sum_fee = 0
    for record in his_records:
        if his_records[record] < 0:
            sum_fee += his_records[record]
    print(sum_fee)
    wx_records = self.get_wx_records(remove_wx_zero=True)
    sum_fee = 0
    for record in wx_records:
        if wx_records[record] < 0:
            sum_fee += wx_records[record]
    print(sum_fee)
