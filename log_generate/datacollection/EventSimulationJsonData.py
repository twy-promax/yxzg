#!/usr/bin/env python
# @desc : 模拟生成日志数据
__coding__ = "utf-8"
__author__ = "itcast team"

import json
import random
import string
import time
from datetime import datetime, timedelta
from faker import Faker

import requests

from log_generate.utils import FileUtils

goods_types = [
    # 商品的大类别
    "蔬菜",
    "水果",
    "肉禽蛋品",
    "干杂调料",
    "粮油米面",
    "休闲食品"
]

# 商品名称
vegetables = [
    "2904431=土豆",
    "2904377=西红柿",
    "2906767=本地黄瓜",
    "2906796=圆茄子",
    "2908032=扁豆角",
    "2904384=青丝瓜",
    "2904429=西葫芦",
    "2904352=菠菜",
    "2904528=大白菜",
    "2904362=紫甘蓝",
    "2904404=西兰花"
]

fruits = [
    "2906404=国产香蕉",
    "2907193=青苹果",
    "2906364=砂糖桔",
    "2907382=鸭梨",
    "2907383=雪花梨",
    "2908123=凯特芒果",
    "2906393=哈密瓜",
    "2907408=京欣西瓜",
    "2906496=樱桃",
    "3222714=黄金柚（个）",
    "2912138=特价菠萝",
    "2906386=水蜜桃"
]

meat = [
    "2904146=五花肉",
    "2904144=前腿肉",
    "2904145=后腿肉",
    "2904175=梅花肉",
    "2905391=鲜羊排",
    "2904282=鸡胸肉（冻）",
    "2904280=鸡翅中（冻）",
    "2907818=鲜鸡大腿",
    "2905376=红壳鸡蛋",
    "2907884=柴鸡蛋",
    "3203636=咸鸭蛋"
]

seasoning = [
    "0300083=东古酱油一品鲜酱油500ml",
    "0301079=海天酱油老抽王500ml",
    "0301727=中盐精致食用盐（加碘）500g",
    "0300789=太太乐鸡精100g",
    "2905131=大桥味精90g",
    "3238062=水塔陈醋800ml",
    "2905064=海天上等蚝油700g",
    "0300514=李锦记蒜蓉辣椒酱226g",
    "0301610=利民甜面酱450g"
]

grain = [
    "2905003=面粉",
    "2909982=东北大米",
    "2905199=小米",
    "0300203=金沙河刀削挂面1kg",
    "0300019=陈克明高筋细圆挂面1kg",
    "0301735=福临门大豆油5L",
    "2905300=鲁花5S压榨一级花生油5L",
    "2905289=福临门100%纯芝麻香油220ml",
    "2905356=鲁花压榨剥壳去皮葵花仁油5L"
]

snacks = [
    "3228379=卡妙奇番茄味薯片408g",
    "3203841=黄油饼干",
    "3226006=萌潮人椒盐酥饼干",
    "3234666=卡慕乳酪夹心饼干600g",
    "3225986=和和园小薯点薯片饼干系列",
    "3238225=澳美思黄油味曲奇饼干308g",
    "3225981=乐阳路梳打饼干系列",
    "3226187=益宾兴蒸枣沙蛋糕",
    "3226185=柯宇蛋糕系列"
]

ids = [
    139040, 196941, 202025, 584793, 1138207, 1390894, 1452990, 1527149, 1558681, 1761246, 1859638, 1871891, 2012817,
    2147276, 2148447, 2150440, 2166387, 2216658, 2217610, 2235717, 2328532, 2334178, 2377481, 2379637, 2429117, 2429195,
    2431263, 2431861, 2433618, 2436003, 2436863, 2621087, 2710461, 2812285, 2980625, 2981519, 2981743, 2986691, 2994317,
    2996662, 3016454, 3018097, 3019037, 3020224, 3118081, 3202725, 3219699, 3226698, 3256691, 3340026, 3341149, 3347256,
    3348998, 3351583, 3351649, 3352427, 3403587, 3405281, 3405860, 3591093, 3591799, 3605583, 3606383, 3641697, 3663217,
    3716932, 3721825, 3735457, 3736435, 3917388, 3921787, 3926880, 3928375, 3985259, 3992361, 4053529, 4057978, 4116334,
    4119396, 4121572, 4131639, 4132946, 4133991, 4134027, 4387718, 4389375, 4474719, 4475616, 4599938, 4605391, 4605945,
    4607951, 4608453, 4612662, 4613276, 4615526, 4632080, 4636605, 4801595, 4803571, 4804075, 4805440, 4842091, 4866141,
    4869985, 4913239, 4918036, 5046700, 5048340, 5057424, 5122142, 5124577, 5125947, 5129935, 5158754, 5160229, 5160709,
    5161660, 5297360, 5341714, 5370644, 5371454, 5441102, 5446536, 5457136, 5461554, 5463298, 5758515, 5824295, 5825860,
    5837447, 5873398, 5874484, 5876480, 5917303, 5930690, 5941742, 6174353, 6236032, 6236364, 6239086, 6305971, 6310156,
    6333507, 6466793, 6597758, 6601411, 6657431, 6719317, 6719486, 6721288, 6725225, 6726793, 6727515, 6784422, 6856668,
    6896885, 6991210, 7074153, 7074934, 7077168, 7077316, 7138246, 7273242, 7637196, 7637637, 7813637, 8103913, 8154333,
    8328247, 8476325, 8476453, 8476636, 8678035, 8718718, 8798492, 8946815, 8960452, 9060977, 9084008, 9087947, 9568505,
    9568575, 9635797, 9651063, 9821156, 9822801, 10000337, 10142159, 10147276, 10171573, 10223700, 10226063, 10266827,
    10268341, 10273613, 10343372, 10343990, 10353363, 10354829, 10578761, 10579992, 10592081, 10592863, 10667732,
    10672662, 10675150, 10715778, 10772059, 11539011, 11607166, 11727077, 11761369, 11857780, 11871797, 11871875,
    11872418, 11927548, 11930833, 11947113, 11954285, 11964057, 12044348, 12158520, 12247566, 12302995, 12717856,
    13040928, 13047417, 13124138, 13124814, 13229232, 13528826, 13584267, 13585106, 13595158, 13595888, 13602507,
    13603510, 13605311, 13657465, 13665579, 13665715, 13668800, 13916142, 13919123, 13935083, 13944530, 14009290,
    14023258, 14033637, 14044748, 14084307, 14221685, 14508793, 14509987, 14591641, 14670982, 14795464, 14834798,
    14841346, 14852187, 14883897, 14971102, 14975115, 14977685, 15002666, 15045119, 15045226, 15081011, 15194916,
    15194922, 15315587, 15335682, 15335842, 15335893, 15336110, 15336270, 15336865, 15336949, 15337067, 15337341,
    15337379, 15337626, 15338021, 15338217, 15338435, 15338734, 15338822, 15339065, 15340522, 15340777, 15341292,
    15343449, 15343610, 15343717, 15344774, 15345341, 15345344, 15346012
]

browse_page = [
    "https://xtx.itcast.cn/home.html",
    "https://xtx.itcast.cn/aboutUs.html",
    "https://xtx.itcast.cn/contactUs.html",
    "https://xtx.itcast.cn/help.html",
    "https://xtx.itcast.cn/login.html",
    "https://xtx.itcast.cn/register.html",
    "https://xtx.itcast.cn/cart.html",
    "https://xtx.itcast.cn/category.html",
    "https://xtx.itcast.cn/search.html",
    "https://xtx.itcast.cn/specialOffers.html",
    "https://xtx.itcast.cn/privacyPolicy.html",
    "https://xtx.itcast.cn/termsOfService.html",
    "https://xtx.itcast.cn/faq.html",
    "https://xtx.itcast.cn/blog.html",
    "https://xtx.itcast.cn/news.html",
    "https://xtx.itcast.cn/promotions.html",
    "https://xtx.itcast.cn/recipes.html",
    "https://xtx.itcast.cn/storeLocator.html",
    "https://xtx.itcast.cn/rewards.html",
    "https://xtx.itcast.cn/referAFriend.html",
    "https://xtx.itcast.cn/productDetail.html?code=asexwxwaxe",
    "https://xtx.itcast.cn/productDetail.html?code=bscyujnpla",
    "https://xtx.itcast.cn/productDetail.html?code=cfvwmbzixy",
    "https://xtx.itcast.cn/productDetail.html?code=dytrznoiwq",
    "https://xtx.itcast.cn/productDetail.html?code=egfjkabmcd",
    "https://xtx.itcast.cn/productDetail.html?code=fjklqwerty",
    "https://xtx.itcast.cn/productDetail.html?code=gzxswnmruo",
    "https://xtx.itcast.cn/productDetail.html?code=hbvuznjmxe",
    "https://xtx.itcast.cn/productDetail.html?code=iqytrwfksd",
    "https://xtx.itcast.cn/productDetail.html?code=jnxzpleawk",
    "https://xtx.itcast.cn/productDetail.html?code=klmgdzcuiv",
    "https://xtx.itcast.cn/productDetail.html?code=lwqayxnpom",
    "https://xtx.itcast.cn/productDetail.html?code=mznqlkewxr",
    "https://xtx.itcast.cn/productDetail.html?code=nabcsyopwx",
    "https://xtx.itcast.cn/productDetail.html?code=ojklmfqzwr",
    "https://xtx.itcast.cn/productDetail.html?code=pivcxrjzkl",
    "https://xtx.itcast.cn/productDetail.html?code=qnwxjlasop",
    "https://xtx.itcast.cn/productDetail.html?code=robmcznsxa",
    "https://xtx.itcast.cn/productDetail.html?code=skvwxdzyon",
    "https://xtx.itcast.cn/productDetail.html?code=tlmnxzsrip",
    "https://xtx.itcast.cn/productDetail.html?code=uvzbxomwlp",
    "https://xtx.itcast.cn/productDetail.html?code=vwqoskenrc",
    "https://xtx.itcast.cn/productDetail.html?code=wzlkjxqenp",
    "https://xtx.itcast.cn/productDetail.html?code=xmalncjopq",
    "https://xtx.itcast.cn/productDetail.html?code=yzxncwoeil",
    "https://xtx.itcast.cn/productDetail.html?code=azmntwlrkd",
    "https://xtx.itcast.cn/productDetail.html?code=blwsopzxne",
    "https://xtx.itcast.cn/productDetail.html?code=cmqolksjfr",
    "https://xtx.itcast.cn/productDetail.html?code=dkxvwmrnsl",
    "https://xtx.itcast.cn/productDetail.html?code=elnszwjvkp",
    "https://xtx.itcast.cn/productDetail.html?code=fzxklwmoer",
    "https://xtx.itcast.cn/productDetail.html?code=gnalmtvjop",
    "https://xtx.itcast.cn/productDetail.html?code=hrbqixzjlm",
    "https://xtx.itcast.cn/productDetail.html?code=isyvnmwklo",
    "https://xtx.itcast.cn/productDetail.html?code=jopwcnskzl",
    "https://xtx.itcast.cn/productDetail.html?code=kqwrmznloy",
    "https://xtx.itcast.cn/productDetail.html?code=lxvqkpmzie",
    "https://xtx.itcast.cn/productDetail.html?code=myzxopnklw",
    "https://xtx.itcast.cn/productDetail.html?code=nxaljcmwzr",
    "https://xtx.itcast.cn/productDetail.html?code=oyqwrnvzxj"
]

# 浏览页面关键词
page_keywords = [
    "性价比",
    "新鲜",
    "好吃",
    "不贵",
    "质量好",
    "美味",
    "实惠",
    "优质"
]


# 生成用户电话号码
def random_phone_number():
    """
     11 位手机号码的组成规律，即：
        手机号码一共有 11 位，以 1 开头
        第 2 位的数值是 3、4、5、7、8 中的一个
        第 3 位根据第 2 位的数字，对应运营商的生成规律
        后 8 位是随机生成的 8 个数字
    :return: phone_number
    """
    # 第二位数字
    second = [3, 4, 5, 7, 8][random.randint(0, 4)]
    # 第三位数字
    third = {3: random.randint(0, 9),
             4: [5, 7, 9][random.randint(0, 2)],
             5: [i for i in range(10) if i != 4][random.randint(0, 8)],
             7: [i for i in range(10) if i not in [4, 9]][random.randint(0, 7)],
             8: random.randint(0, 9), }[second]
    # 最后八位数字
    suffix = random.randint(9999999, 100000000)
    # 拼接手机号
    return "1{}{}{}".format(second, third, suffix)

# 生成系统用户名
def random_system_uname(length=11):
    return "".join(
        [random.choice(string.ascii_letters) if random.randint(0, 1) else random.choice(string.digits) for _ in
         range(length)])

# 生成地址
def random_phone_area(phone):
    """
    判断手机号码是否合理
    :param phone:手机号码
    :return:
    https://www.baifubao.com/callback?cmd=1059&callback=phone&phone=%s
    """
    resp_content = requests.get('https://cx.shouji.360.cn/phonearea.php?number=%s' % phone).content
    txt = json.loads(resp_content)
    province = txt.get('data').get("province")
    city = txt.get('data').get("city")
    sp = txt.get('data').get("sp")
    area_result = '{left_brackets}"province":"{province}", "city":"{city}", "sp":"{sp}"{right_brackets}' \
        .format(left_brackets="{",
                province=province,
                city=city,
                sp=sp,
                right_brackets="}"
                )
    return area_result


# 生成user_id
def random_user_id():
    return random.sample(ids, 1)[0]


# 生成访问时间
def random_access_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# 生成商品类别
def random_goods_type():
    return random.sample(goods_types, 1)[0]


# 生成商品价格
def random_minimum_price():
    return float("{:.2f}".format(random.uniform(1, 10)))


# 生成用户访问行为
def random_user_behavior():
    """
        行为：浏览、下单、购买、退单、加购
    :return:
    """
    if random.uniform(0, 1) >= 0.4:
        behavior_json = '{"is_browse":1, "is_order":0, "is_buy":0, "is_back_order":0, "is_cart":0}'
    elif 0.2 <= random.uniform(0, 1) < 0.4:
        behavior_json = '{"is_browse":0, "is_order":0, "is_buy":0, "is_back_order":0, "is_cart":1}'
    elif 0.1 <= random.uniform(0, 1) < 0.2:
        behavior_json = '{"is_browse":0, "is_order":1, "is_buy":0, "is_back_order":0, "is_cart":0}'
    elif 0.05 <= random.uniform(0, 1) < 0.1:
        behavior_json = '{"is_browse":0, "is_order":0, "is_buy":1, "is_back_order":0, "is_cart":0}'
    else:
        behavior_json = '{"is_browse":0, "is_order":0, "is_buy":0, "is_back_order":1, "is_cart":0}'
    return behavior_json


# 生成商品明细信息
def random_goods_detail(goods_type):
    """
    goods_name 商品
    browse_page 浏览页面
    browse_time 浏览时间
    to_page 跳转页面
    to_time 跳转时间
    page_keywords 页面关键词
    :return:
    """
    if goods_type == '蔬菜':
        goodsName = random.sample(vegetables, 1)[0]
    elif goods_type == '水果':
        goodsName = random.sample(fruits, 1)[0]
    elif goods_type == '肉禽蛋品':
        goodsName = random.sample(meat, 1)[0]
    elif goods_type == '干杂调料':
        goodsName = random.sample(seasoning, 1)[0]
    elif goods_type == '粮油米面':
        goodsName = random.sample(grain, 1)[0]
    else:
        goodsName = random.sample(snacks, 1)[0]

    pages = random.sample(browse_page, 2)
    browsePage = pages[0]
    toPage = pages[1]
    browseTime = (datetime.now() + timedelta(minutes=-25)).strftime("%Y-%m-%d %H:%M:%S")
    toTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if random.uniform(0, 1) < 0.5:
        pageKeywords = "-"
    else:
        pageKeywords = random.sample(page_keywords, 1)[0]
    goods_detail = '{left_brackets}"goods_name":"{goods_name}", "browse_page":"{browse_page}", "browse_time":"{browse_time}", "to_page":"{to_page}", "to_time":"{to_time}", "page_keywords":"{page_keywords}"{right_brackets}' \
        .format(left_brackets="{",
                goods_name=goodsName,
                browse_page=browsePage,
                browse_time=browseTime,
                to_page=toPage,
                to_time=toTime,
                page_keywords=pageKeywords,
                right_brackets="}")
    return goods_detail


# http://nginx.org/en/docs/http/ngx_http_log_module.html
def generate_user_event_json(count=100):
    """
    生成用户事件json数据格式：10个字段
    {'phone_num': '13771667558', 'system_id': 'o943iQOjB6F', 'area': "{'province': '江苏', 'city': '苏州', 'sp': '移动'}", 'user_name': '王琳', 'user_id': '6219-2060541', 'visit_time': '2022-05-06 12:05:21', 'goods_type': '企业团体险', 'minimum_price': '8283.07', 'user_behavior': "{'is_browse':1, 'is_order':0, 'is_buy':0, 'is_back_order':0, 'is_cart':0}", 'goods_detail': "{'goods_name': '平安中老年综合意外险', 'browse_page': 'https://baoxian.sanyou.com/pa18shopnst/nstShop/index.html#/productInfo/ZP020501?WT.mc_id=T00-WT-PAHOME-testB-cp66', 'browse_time': '2022-05-05 12:05:21', 'to_page': 'https://baoxian.sanyou.com/pa18shopnst/nstShop/index.html#/productInfo/ZP020501?WT.mc_id=T00-WT-PAHOME-baoxian-jiankangxian-all', 'to_time': '2022-05-06 12:05:21', 'page_keywords': '家庭财产保险'}"}
    :param count: 日志数量，默认生成100条
    """
    while count >= 1:
        phone_num = random_phone_number()
        user_id = random_user_id()
        goods_type = random_goods_type()
        event_json = '{left_brackets}"phone_num": "{phone_num}","system_id": "{system_id}","area": {area_json},"user_name": "{user_name}","user_id": "{user_id}","visit_time": "{visit_time}","goods_type": "{goods_type}","minimum_price": {minimum_price},"user_behavior": {user_behavior},"goods_detail": {goods_detail}{right_brackets}' \
            .format(left_brackets="{",
                    phone_num=phone_num,
                    system_id=random_system_uname(),
                    area_json=random_phone_area(phone_num),
                    user_name=user_id,
                    user_id=user_id,
                    visit_time=random_access_time(),
                    goods_type=goods_type,
                    minimum_price=random_minimum_price(),
                    user_behavior=random_user_behavior(),
                    goods_detail=random_goods_detail(goods_type),
                    right_brackets="}"
                    )
        print(event_json)
        FileUtils.writeDataToFile('./source_data', 'user-event', event_json)
        # FileUtils.writeDataToGZFile('./source_data', 'user-event', event_json)
        # linux env
        # FileUtils.writeDataToFile('/root/InsuranceUserProfile/source_data', 'user-event', event_json)
        count = count - 1


if __name__ == '__main__':
    generate_user_event_json(5)  # 传入5就随机生成5条数据
    # line = readDataFromUserEvent()
    # jsonObj = json.dumps(line, ensure_ascii=False)
    # print(jsonObj)
    # print(json.loads(jsonObj, encoding="utf-8"))
    # for line in FileUtils.readDataFromFile('./source_data', 'user-event'):
    #     print(line, end='')
    # for line in FileUtils.readDataFromGZFile('./source_data', 'user-event'):
    #     print(line, end='')
