from datetime import datetime

import pandas as pd

from tool.ReconciliationTools import ReconciliationTools

base_dir = 'D:\\DYT - JobFile\\市二院\\5.9账单\\'
his_records_file_name = '5.9滇医通订单明细.xls'
wx_records_file_name = '1657259023All2024-05-09_2024-05-09.csv'
recon_date = datetime(2024, 5, 9)

tool = ReconciliationTools(base_dir, wx_records_file_name, recon_date)


def execute():
    his_records = get_his_records()
    wx_records = tool.get_wx_records()
    tool.wx_verify_his(wx_records, his_records)
    tool.his_verify_wx(his_records, wx_records)


def get_his_records():
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, header=5, skipfooter=1)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict['单号']):
        if his_records_data_dict['单号'][index] in his_records_dict:
            his_records_dict[his_records_data_dict['单号'][index]] += his_records_data_dict['金额'][index]
        else:
            his_records_dict[his_records_data_dict['单号'][index]] = his_records_data_dict['金额'][index]
        index += 1
    return his_records_dict


if __name__ == '__main__':
    execute()
