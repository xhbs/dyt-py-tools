from datetime import datetime, timedelta
from typing import TypeVar, Union

import pandas as pd

from province_dental.CommonUtils import is_right_order_no
from tool.ReconciliationTools import ReconciliationTools

IntStrT = TypeVar("IntStrT", bound=Union[int, str])

base_dir = 'D:\\滇医通\\省口腔对账\\11月对账\\'
his_records_file_name = '滇医通13-14日挂号记录.xlsx'
wx_records_file_name = '1523561421All2024-11-13_2024-11-14.csv'
recon_daterange_start = datetime(2024, 11, 13)
recon_daterange_end = datetime(2024, 11, 14)


def execute():
    recon_date = recon_daterange_start
    while recon_date <= recon_daterange_end:
        print(f'对账日期:{recon_date}')
        tool = ReconciliationTools(base_dir, wx_records_file_name, recon_date)
        his_records = get_his_records(recon_date=recon_date)
        wx_records = tool.get_wx_records(remove_wx_zero=True)
        tool.wx_verify_his(wx_records, his_records)
        tool.his_verify_wx(his_records, wx_records)
        recon_date = recon_date + timedelta(days=1)
        print()


def get_his_records(recon_date: datetime,
                    sheet_name: str | int | list[IntStrT] | None = 0,
                    order_no_column: str = '流水号',
                    date_column: str = '收费日期',
                    date_format: str = '%Y-%m-%d %H:%M:%S',
                    fee_column: str = '费用合计', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict[order_no_column]):
        if not is_right_order_no(his_records_data_dict[order_no_column][index]):
            index += 1
            continue
        ope_date = datetime.strptime(his_records_data_dict[date_column][index], date_format)
        ope_date = ope_date.strftime('%Y-%m-%d')
        if ope_date != recon_date.strftime('%Y-%m-%d'):
            index += 1
            continue
        fee = his_records_data_dict[fee_column][index]
        if fee == 0:
            if his_records_data_dict[order_no_column][index] in his_records_dict:
                his_records_dict.pop(his_records_data_dict[order_no_column][index])
            index += 1
            continue
        his_records_dict.update(
            {his_records_data_dict[order_no_column][index]: his_records_data_dict[fee_column][index]})
        index += 1
    return his_records_dict


if __name__ == '__main__':
    execute()
