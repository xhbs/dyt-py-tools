import pandas as pd


def execute():
    data_frame = pd.read_excel(r"D:\DYT - JobFile\延安医院\对账\2024-07\01~11\医院核对结果\滇医通.xlsx")
    excel_dict = data_frame.to_dict()
    appoint_status_list = excel_dict['就诊状态']
    fee_list = excel_dict['金额']
    order_no_list = excel_dict['滇医通订单号']

    count = 0
    count_1 = 0
    total_fee: float = 0
    total_fee_1: float = 0
    order_no_str = str()
    for index in appoint_status_list:
        if appoint_status_list[index] == '已诊':
            count += 1
            total_fee += float(fee_list[index])
            order_no_str = order_no_str + ", '" + order_no_list[index] + "'"
        else:
            count_1 += 1
            total_fee_1 += float(fee_list[index])
    print(count)
    print(total_fee)
    print()
    print(count_1)
    print(total_fee_1)


if __name__ == '__main__':
    execute()
