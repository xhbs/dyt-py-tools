import mysql.connector
import redis

# MySQL数据库连接配置
mysql_config = {
    'host': '192.168.1.228',
    'user': 'dyt',
    'password': 'dyt',
    'database': 'dyt_tenant'
}

# Redis连接配置
redis_config = {
    'host': '192.168.1.227',
    'port': 6380,
    'password': 'xQ3F5cRX6X',
    'db': 0
}


def fetch_data_from_mysql():
    # 连接MySQL数据库
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    # 执行查询语句
    query = "SELECT phone_number FROM t_tenant_user where name = 'super_admin'"
    cursor.execute(query)
    # 获取查询结果
    results = cursor.fetchall()
    # 关闭连接
    cursor.close()
    cnx.close()
    return results


def fetch_data_from_redis(key):
    # 连接Redis数据库
    r = redis.Redis(**redis_config)
    # 查询Redis中的key
    value = r.get(key)
    return value


def main1():
    # 从MySQL数据库获取数据
    mysql_results = fetch_data_from_mysql()
    for result in mysql_results:
        # 假设result是一个包含字段值的元组，例如(result[0], result[1], ...)
        field_value = result[0]  # 选择你需要的字段
        # 拼接Redis的key
        redis_key = f"dyt-user:user:users:phone:{field_value}:login"
        # 从Redis中获取对应的值
        redis_value = fetch_data_from_redis(redis_key)
        # 打印Redis中的值
        print(f"Redis key: {redis_key}, Redis value: {redis_value}")


def main2():
    # 拼接Redis的key
    redis_key = f"dyt-user:user:users:phone:15887503357:login"
    # 从Redis中获取对应的值
    redis_value = fetch_data_from_redis(redis_key)
    # 打印Redis中的值
    print(f"Redis key: {redis_key}, Redis value: {redis_value}")


if __name__ == "__main__":
    main2()
