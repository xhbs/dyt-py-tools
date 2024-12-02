import mysql.connector
import pandas as pd

mysql_config = {
    'host': '119.62.102.80',
    'port': 8887,
    'user': 'root',
    'password': '123456',
    'database': 'scale_breathe'
}


def query_breath():
    file_pd = pd.read_excel(
        r"D:\Users\dzhon\Documents\WeChat Files\wxid_6091750917712\FileStorage\File\2024-07\0a4aaf733e2c48124630a0c7003378c0_68655d460c1040b094ab7f37b964ac5c_8.xlsx")
    file_dict = file_pd.to_dict()
    zy_no_list = list(file_dict['住院号'].values())
    zy_no_list = [str(item).strip() if not isinstance(item, str) else item for item in zy_no_list]
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    order_no_dict = dict[str, str]()
    print(len(zy_no_list))
    zy_no_str = ",".join(zy_no_list)
    # 执行查询语句
    query = f"SELECT `住院号`,`主任医师` FROM jhdl_10_hospital_gathering where `住院号` in ({zy_no_str})"
    cursor.execute(query)
    # 获取查询结果
    results = cursor.fetchall()
    print(len(results))
    for result in results:
        # 假设result是一个包含字段值的元组，例如(result[0], result[1], ...)
        field_value = result[0]  # 选择你需要的字段
        order_no_dict.update({field_value: result[1]})

    query = f"SELECT `P_INPATIENTID`,`P_DEPUTY_DOC_NAME` FROM jhdl_23_epr_homepage_baseinfo where `P_INPATIENTID` in ({zy_no_str})"
    cursor.execute(query)
    # 获取查询结果
    results2 = cursor.fetchall()
    print(len(results2))
    for result in results2:
        # 假设result是一个包含字段值的元组，例如(result[0], result[1], ...)
        field_value = result[0]  # 选择你需要的字段
        order_no_dict.update({field_value: result[1]})

    print("************************************")
    query = f"SELECT COUNT(1) FROM jhdl_23_epr_homepage_baseinfo WHERE `P_INPATIENTID` NOT in ({zy_no_str})"
    cursor.execute(query)
    results = cursor.fetchall()
    print(len(results))
    for result in results:
        print(result)
    # 关闭连接
    cursor.close()
    cnx.close()

    print('-------------------------------------')
    for zyh in order_no_dict:
        name_temp = str(order_no_dict[zyh])
        if '_' in name_temp:
            name_temp = name_temp.split('_')[0]
            order_no_dict[zyh] = name_temp
        if zyh not in zy_no_list:
            print(zyh)

    df = pd.DataFrame.from_dict(order_no_dict, orient='index', columns=['主任医师'])
    df.index.name = '住院号'
    result_path = r'D:\lib\temp\主任医师.xlsx'
    df.to_excel(result_path)


if __name__ == '__main__':
    query_breath()
