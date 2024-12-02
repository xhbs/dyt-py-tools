import pandas as pd
import requests


def excute():
    order_dict = read_from_excel()
    index = 0
    while index < len(order_dict['id']):
    # while index < 1:
        order_data = {
            'order_no': order_dict['order_no'][index],
            'total_fee': order_dict['total_fee'][index],
        }
        do_request(order_data)
        index += 1


def read_from_excel():
    excel_dir = 'D:\DYT - JobFile\省口腔\\2024-04-26停诊处理\停诊订单（status = 9 and cancel_status != 9）.xlsx'
    order_list = pd.read_excel(excel_dir, sheet_name='Result 1')
    order_dict = order_list.to_dict()
    return order_dict


def do_request(order_data: dict):
    print(order_data)
    request_url = 'https://appv3-api.ynhdkc.com/rpc/v1/payment-center/refund/wechat'
    # 设置请求头
    headers = {
        'Authorization': 'Basic ZHl0OkwyRDNXdUJIcnB4TXZ6elA=',
        'Content-Type': 'application/json'
    }
    # 设置请求体
    data = {
        'order_no': order_data['order_no'],
        'reason': '医院停诊，手动流转',
        'total_fee': order_data['total_fee']
    }

    # 发送POST请求
    response = requests.post(request_url, headers=headers, json=data)
    if response.status_code == 200:
        result = order_data['order_no'] + '         success\n'
    else:
        result = order_data['order_no'] + '         failed\n'
    with open("D:\DYT - JobFile\省口腔\\2024-04-26停诊处理\handle_result.txt", 'a') as file:
        file.write(result)


if __name__ == '__main__':
    excute()
