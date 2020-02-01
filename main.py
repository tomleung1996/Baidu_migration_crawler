import requests
import json
import pymongo
from tqdm import tqdm


def fetch_single_data(level, move_in, location_id, date):
    """
    爬取百度迁徙的人口流入/流出分布情况和绝对数字（数字含义未知，应能表示相对大小）

    已知到了每天的12点之后才会更新昨天的数据，县级市的数据不会单列

    :param level: 人口流入/流出的行政单位，市级或省级
    :param move_in: 人口流入/流出 True or False
    :param location_id: 目标行政单位，市级或省级
    :param date: 目标日期，格式为yyyymmdd
    :return:
    """

    # 市级分布
    city_dis_url = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt={0}&id={1}&type={2}&date={3}'

    # 省级分布
    province_dis_url = 'http://huiyan.baidu.com/migration/provincerank.jsonp?dt={0}&id={1}&type={2}&date={3}'

    # 绝对数字
    number_url = 'http://huiyan.baidu.com/migration/historycurve.jsonp?dt={0}&id={1}&type={2}&startDate={3}&endDate={3}'

    if move_in:
        move = 'move_in'
    else:
        move = 'move_out'

    if str(location_id).endswith('0000'):
        dtype = 'province'
    else:
        dtype = 'city'

    if level == 'city':
        dis_url = city_dis_url.format(dtype, location_id, move, date)
    else:
        dis_url = province_dis_url.format(dtype, location_id, move, date)

    number_url = number_url.format(dtype, location_id, move, date)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
    }

    dis_result = requests.get(dis_url, headers=headers).text

    number_result = requests.get(number_url, headers=headers).text

    start_1 = dis_result.find('(')
    end_1 = dis_result.rfind(')')
    start_2 = number_result.find('(')
    end_2 = number_result.rfind(')')

    try:
        dis_result = json.loads(dis_result[start_1+1:end_1])['data']['list']
        number_result = json.loads(number_result[start_2+1:end_2])['data']['list'][date]

        final_result = {
            'number': float(number_result),
            'distribution': dis_result
        }
    except KeyError:
        print('KeyError, 提交请求格式可能有问题')
        return None
    except TypeError:
        print('TypeError, 可能遇到县级市')
        return None

    return final_result


def fetch_all_data(loc_file_path:str):
    loc_id_dict = {}
    res = []

    with open(loc_file_path, mode='r', encoding='utf-8') as file:
        for line in file:
            loc, code = line.strip().split(',')
            loc_id_dict[int(code)] = loc

    for code, loc in tqdm(loc_id_dict.items()):
        print(code, loc)
        for level in ['city', 'province']:
            for move_in in [True, False]:
                single_res = fetch_single_data(level, move_in, code, '20200131')
                if single_res is None:
                    break

                res.append({
                    'location': loc,
                    'code': code,
                    'type': 'province' if str(code).endswith('0000') else 'city',
                    'level': level,
                    'move': 'in' if move_in else 'out',
                    'date': '20200131',
                    'number': single_res['number'],
                    'distribution': single_res['distribution']
                })
            else:
                continue
            break

    return res


if __name__ == '__main__':
    # fetch_single_data('city', True, '110000', '20200130')
    result = fetch_all_data(r'location_ids.txt')

    # connect_str = "mongodb://tomleung1996:1996821abc@baiduqianxi-shard-00-00-n9mzn.azure.mongodb.net:27017,baiduqianxi-shard-00-01-n9mzn.azure.mongodb.net:27017,baiduqianxi-shard-00-02-n9mzn.azure.mongodb.net:27017/test?ssl=true&replicaSet=BaiduQianxi-shard-0&authSource=admin&retryWrites=true&w=majority"
    # client = pymongo.MongoClient(connect_str)
    # client.qianxi.flow.insert_many(result)
    # client.close()