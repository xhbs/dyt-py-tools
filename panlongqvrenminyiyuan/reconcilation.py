from datetime import datetime, timedelta
from typing import TypeVar, Union

import pandas as pd

from tool.CommonUtils import is_right_order_no
from tool.ReconciliationTools import ReconciliationTools

IntStrT = TypeVar("IntStrT", bound=Union[int, str])

base_dir = 'D:\\滇医通\\官渡区人民医院\\官渡区人民医院对账\\滇医通10月对账\\滇医通10月错账明细'
his_records_file_name = '\\滇医通 his 10-19.xlsx'
wx_records_file_name = 'D:\\滇医通\\官渡区人民医院\\官渡区人民医院对账\\滇医通10月对账\\1640682391All2024-10-19_2024-10-19.csv'
recon_daterange_start = datetime(2024, 10, 19)
recon_daterange_end = datetime(2024, 10, 19)


def execute():
    recon_date = recon_daterange_start
    while recon_date <= recon_daterange_end:
        print(f'对账日期:{recon_date}')
        tool = ReconciliationTools("", wx_records_file_name, recon_date)
        his_records = get_his_records(recon_date=recon_date,order_no_column='交易流水号',date_column='收款时间',fee_column='交易金额',date_format='%Y-%m-%d')
        wx_records = tool.get_wx_records(remove_wx_zero=True)
        tool.wx_verify_his(wx_records, his_records)
        tool.his_verify_wx(his_records, wx_records)
        recon_date = recon_date + timedelta(days=1)
        print()


def get_his_records(recon_date: datetime,
                    sheet_name: str | int | list[IntStrT] | None = 0,
                    order_no_column: str = '流水号',
                    date_column: str = '操作日期',
                    date_format: str = '%Y-%m-%d',
                    fee_column: str = '金额', ):
    his_records_excel_data = pd.read_excel(base_dir + his_records_file_name, sheet_name=sheet_name,header=2)
    his_records_data_dict = his_records_excel_data.to_dict()
    index = 0
    his_records_dict = dict()
    while index < len(his_records_data_dict[order_no_column]):
        if not is_right_order_no(his_records_data_dict[order_no_column][index]):
            index += 1
            continue

        fee = his_records_data_dict[fee_column][index]
        lastFee = his_records_dict.get(his_records_data_dict[order_no_column][index])
        fee = fee + (lastFee if lastFee is not None else 0)
        if fee == 0:
            if his_records_data_dict[order_no_column][index] in his_records_dict:
                his_records_dict.pop(his_records_data_dict[order_no_column][index])
            index += 1
            continue

        his_records_dict.update(
            {his_records_data_dict[order_no_column][index]: fee})
        index += 1
    return his_records_dict


if __name__ == '__main__':
    execute()
