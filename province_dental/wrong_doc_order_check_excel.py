import pandas as pd


def excute():
    base_dir = 'D:\DYT - JobFile\省口腔\订单医生姓名错误\\'
    order_excel = base_dir + 'dyt_appointment_order_t_order.xlsx'
    doctor_excel = base_dir + 'dyt_tenant_t_doctor.xlsx'
    result_file = base_dir + 'result.txt'
    order_list = pd.read_excel(order_excel, sheet_name='Result 1')
    doctor_list = pd.read_excel(doctor_excel, sheet_name='Result 1')
    order_excel_dict = order_list.to_dict()
    doctor_excel_dict = doctor_list.to_dict()

    docDic = dict()
    index = 0
    while index < len(doctor_excel_dict['id']):
        docDic[doctor_excel_dict['thrdpart_doctor_code'][index]] = {
            'name': doctor_excel_dict['name'][index],
            'id': doctor_excel_dict['id'][index],
            'code': doctor_excel_dict['thrdpart_doctor_code'][index]
        }
        index += 1

    index = 0
    while index < len(order_excel_dict['id']):
        doc_name = order_excel_dict['doctor_name'][index]
        doc_code = order_excel_dict['doctor_code'][index]
        if docDic[doc_code]['name'] != doc_name:
            result = str(order_excel_dict['order_no'][index]) + "    " + str(order_excel_dict['doctor_code'][index]) + "    " + order_excel_dict['doctor_name'][index] + "    " + docDic[doc_code]['name'] + "\n"
            with open(result_file, 'a',encoding='utf-8') as file:
                file.write(result)
        index += 1


if __name__ == '__main__':
    excute()
