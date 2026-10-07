#!/usr/bin/env python
# @desc : 模拟生成日志数据
__coding__ = "utf-8"
__author__ = "itcast team"

import random
import time

# url地址集合
from log_generate.utils import FileUtils

url_paths = [
    "pa18shopnst/nstShop/index.html",
    "pa18shopnst/nstShop/128.html",
    "zaixiangoumai/chexian/chexian.shtml",
    "pa18shopnst/nstShop/index.html#/productInfo/resources/list?type=FILE&_t=0.7003008955390666",
    "nstShop/select-by-id?processInstanceId=7&_t=0.21326670559456007/detail.html",
    "pa18shopnst/nstShop/index.html#/productInfo/ZP020501?WT.mc_id=T00-WT-PAHOME-testB-cp35",
    "zaixiangoumai/chexian/chexian.shtml?taskID=85268&mailID=2065153389",
    "pa18shopnst/nstShop/list",
    "css/40.30d6d2b.css",
    "css/1.500d1d0.css",
    "js/40.601658f.js",
    "js/20.b9c086d.js",
    "images/dag_bg.png?6a0c3839385c7d50f21acf06989addf4",
    "images/toolbar_SHELL.png?249b36a2f0687b942c0dc26e85bf5c5d",
    "home.html",
    "aboutUs.html",
    "contactUs.html",
    "help.html",
    "login.html",
    "register.html",
    "cart.html",
    "category.html",
    "search.html",
    "specialOffers.html",
    "privacyPolicy.html",
    "termsOfService.html",
    "faq.html",
    "blog.html",
    "news.html",
    "promotions.html",
    "recipes.html",
    "storeLocator.html",
    "rewards.html",
    "referAFriend.html",
    "productDetail.html?code=asexwxwaxe"
]

# ip地址数字集合
ip_slices = [132, 168, 175, 10, 23, 179, 187, 224, 73, 29, 90, 169, 48, 89, 120, 67, 138, 168, 220, 221, 98, 17, 192,
             33]

# http请求地址uri
http_uri = [
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
    "https://xtx.itcast.cn/product/search.html?keyword={query}",
    "https://xtx.itcast.cn/onsale/search.html?keyword={query}",
    "https://www.baidu.com/goods-recommend/search.html?keyword={query}",
    "https://www.douyin.com/goods-recommend/search.html?keyword={query}",
    "https://www.toutiao.com/goods-recommend/search.html?keyword={query}",
    "https://www.xiaohongshu.com/goods-recommend/search.html?keyword={query}"

]

# 浏览页面关键词
search_keyword = ["性价比",
                  "新鲜",
                  "好吃",
                  "不贵",
                  "质量好",
                  "美味",
                  "实惠",
                  "优质",
                  "叶菜",
                  "水果",
                  "零食"
                  ]

# http响应状态码集合
status_codes = ["200", "404", "401", "500"]

# user agent集合
user_agent = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Linux; U; Android 8.0.0; zh-cn; MI 6 Build/OPR1.170623.027) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/37.0.0.0 MQQBrowser/7.8 Mobile Safari/537.36",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 1.1.4322)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/1.22 (compatible; MSIE 10.0; Windows 3.1)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 7.1; Trident/5.0)",
    "Opera/9.80 (Windows NT 6.1; U; en-GB) Presto/2.7.62 Version/11.00",
    "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
    "Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36",
    "Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36",
    "Mozilla/5.0 (Windows x86; rv:19.0) Gecko/20100101 Firefox/19.0",
    "Mozilla/5.0 (X11; OpenBSD amd64; rv:28.0) Gecko/20100101 Firefox/28.0",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; en-us) AppleWebKit/534.16+ (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/7.0.18(0x17001233) NetType/WIFI Language/zh_CN",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 MQQBrowser/11.0.7 Mobile/15E148 Safari/604.1 QBWebViewUA/2 QBWebViewType/1 WKType/1",
    "Mozilla/5.0 (Linux; Android 10; POT-AL00a Build/HUAWEIPOT-AL00a; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.6 SP-engine/2.26.0 baiduboxapp/12.6.0.10 (Baidu; P1 10) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 7.1.2; vivo X9Plus Build/N2G47H; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.6 SP-engine/2.26.0 baiduboxapp/12.6.0.10 (Baidu; P1 7.1.2) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 8.1.0; OPPO R11s Build/OPM1.171019.011; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/11.23 SP-engine/2.19.0 baiduboxapp/11.23.5.10 (Baidu; P1 8.1.0) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 9; V1813BA Build/PKQ1.181030.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.5 SP-engine/2.26.0 baiduboxapp/12.5.1.10 (Baidu; P1 9) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 9; BND-AL10 Build/HONORBND-AL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.6 SP-engine/2.26.0 baiduboxapp/12.6.0.10 (Baidu; P1 9) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 5.1.1; vivo X7Plus Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.6 SP-engine/2.26.0 baiduboxapp/12.6.0.10 (Baidu; P1 5.1.1) NABar/1.0",
    "Mozilla/5.0 (Linux; U; Android 9; zh-cn; HWI-TL00 Build/HUAWEIHWI-TL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/77.0.3865.120 MQQBrowser/11.0 Mobile Safari/537.36 COVC/045429",
    "Mozilla/5.0 (Linux; Android 10; PACM00 Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0",
    "Mozilla/5.0 (Linux; Android 8.1.0; MI PLAY Build/O11019; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.6 SP-engine/2.26.0 baiduboxapp/12.6.0.10 (Baidu; P1 8.1.0) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 10; VOG-AL10 Build/HUAWEIVOG-AL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.1 matrixstyle/0 lite baiduboxapp/5.0.0.11 (Baidu; P1 10) NABar/1.0",
    "Mozilla/5.0 (Linux; Android 8.1.0; M1813 Build/O11019; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/77.0.3865.120 MQQBrowser/6.2 TBS/045435 Mobile Safari/537.36 MMWEBID/3782 MicroMessenger/7.0.21.1800(0x27001539) Process/tools WeChat/arm64 Weixin NetType/4G Language/zh_CN ABI/arm64",
    "Mozilla/5.0 (Linux; U; Android 9; zh-CN; MI 9 Build/PKQ1.181121.001) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.108 UCBrowser/13.1.6.1096 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 6.0; EVA-DL00 Build/HUAWEIEVA-DL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/11.23 SP-engine/2.17.0 lite baiduboxapp/4.19.0.10 (Baidu; P1 6.0)",
    "Mozilla/5.0 (Linux; Android 9; ONEPLUS A3010 Build/PKQ1.181203.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.1 matrixstyle/0 lite baiduboxapp/5.0.0.11 (Baidu; P1 9) NABar/1.0",
    "Mozilla/5.0 (Linux; U; Android 10; zh-cn; Redmi K30 5G Build/QKQ1.191222.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/79.0.3945.147 Mobile Safari/537.36 XiaoMi/MiuiBrowser/13.5.26",
    "Mozilla/5.0 (Linux; U; Android 10; zh-CN; MIX 3 Build/QKQ1.190828.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.108 UCBrowser/13.2.0.1100 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; U; Android 10; zh-cn; ONEPLUS A6000 Build/QKQ1.190716.003) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/66.0.3359.126 MQQBrowser/10.1 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 9; HMA-AL00 Build/HUAWEIHMA-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.6 SP-engine/2.26.0 baiduboxapp/12.6.0.10 (Baidu; P1 9) NABar/1.0",
    "Mozilla/5.0 (Linux; U; Android 10; zh-cn; LYA-AL10 Build/HUAWEILYA-AL10) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/77.0.3865.120 MQQBrowser/11.0 Mobile Safari/537.36 COVC/045429"
]


def ip_to_int(ip):
    parts = ip.split('.')
    return (int(parts[0]) << 24) + (int(parts[1]) << 16) + (int(parts[2]) << 8) + int(parts[3])


def int_to_ip(ip_int):
    return f"{(ip_int >> 24) & 0xFF}.{(ip_int >> 16) & 0xFF}.{(ip_int >> 8) & 0xFF}.{ip_int & 0xFF}"


def generate_random_china_ip():
    # 定义中国的IP地址段
    china_ip_ranges = [
        ('36.0.0.0', '36.255.255.255'),
        ('58.0.0.0', '58.255.255.255'),
        ('59.0.0.0', '59.255.255.255'),
        ('60.0.0.0', '60.255.255.255'),
        ('61.0.0.0', '61.255.255.255'),
        ('101.0.0.0', '101.255.255.255'),
        ('103.0.0.0', '103.255.255.255'),
        ('106.0.0.0', '106.255.255.255'),
        ('110.0.0.0', '110.255.255.255'),
        ('111.0.0.0', '111.255.255.255'),
        ('112.0.0.0', '112.255.255.255'),
        ('113.0.0.0', '113.255.255.255'),
        ('114.0.0.0', '114.255.255.255'),
        ('115.0.0.0', '115.255.255.255'),
        ('116.0.0.0', '116.255.255.255'),
        ('117.0.0.0', '117.255.255.255'),
        ('118.0.0.0', '118.255.255.255'),
        ('119.0.0.0', '119.255.255.255'),
        ('120.0.0.0', '120.255.255.255'),
        ('121.0.0.0', '121.255.255.255'),
        ('122.0.0.0', '122.255.255.255'),
        ('123.0.0.0', '123.255.255.255'),
        ('124.0.0.0', '124.255.255.255'),
        ('125.0.0.0', '125.255.255.255'),
        ('159.226.0.0', '159.226.255.255'),
        ('202.0.0.0', '203.255.255.255'),
        ('210.0.0.0', '211.255.255.255'),
        ('218.0.0.0', '219.255.255.255'),
        ('220.0.0.0', '221.255.255.255'),
        ('222.0.0.0', '223.255.255.255'),
    ]

    ip_range = random.choice(china_ip_ranges)
    start_ip_int = ip_to_int(ip_range[0])
    end_ip_int = ip_to_int(ip_range[1])
    random_ip_int = random.randint(start_ip_int, end_ip_int)
    return int_to_ip(random_ip_int)


# 生成访问者来源ip
def random_htv_ip():
    random_china_ip = generate_random_china_ip()
    return random_china_ip


# 生成访问东八时区时间
def random_htv_accessdatetime():
    return time.strftime("[%d/%b/%Y:%H:%M:%S +0800]", time.localtime())


# 生成请求方式
def random_http_reqmethod():
    if random.uniform(0, 1) > 0.1:
        return "GET"
    else:
        return "POST"


# 生成访问url
def random_htv_url():
    return random.sample(url_paths, 1)[0]


# 生成状态码
def random_htv_status_code():
    if random.uniform(0, 1) > 0.8:
        return "200"
    else:
        return random.sample(status_codes, 1)[0]


# 请求的字节大小
def random_byte_size():
    if random.uniform(0, 1) <= 0.6:
        return random.sample(range(1000), 1)[0]
    elif 0.6 < random.uniform(0, 1) <= 0.8:
        return random.sample(range(10000), 1)[0]
    elif 0.8 < random.uniform(0, 1) <= 1:
        return random.sample(range(100000), 1)[0]
    else:
        return random.sample(range(1000000), 1)[0]


# 生成访问uri
def random_htv_referer():
    if random.uniform(0, 1) < 0.3:
        return "-"
    refer_str = random.sample(http_uri, 1)
    query_str = random.sample(search_keyword, 1)
    return refer_str[0].format(query=query_str[0])


# 生成useragent
def random_user_agent():
    return random.sample(user_agent, 1)[0]


# 生成代理服务器地址
def random_htv_proxy_ip():
    if random.uniform(0, 1) < 0.99:
        return "-"
    else:
        slice = random.sample(ip_slices, 4)
        return ".".join([str(item) for item in slice])


# 写入日志数据到文件中
def writeNginxLogDataToFile(nginx_log=""):
    with open(f'./access-nginx-{time.strftime("%Y%m%d", time.localtime())}', 'a', encoding="utf-8") as fileHandler:
        fileHandler.write(nginx_log + "\n")
    fileHandler.close()


# http://nginx.org/en/docs/http/ngx_http_log_module.html
def generate_nginx_log(count=100):
    """
    生成nginx日志,格式：10个字段
        访问者来源ip 第二个"-"无具体意义 远程客户端用户名称，一般为"-" 访问时间 http请求方法 请求地址（URI） http状态码 本次请求的字节大小
        refer信息 客户端ua标识 http_x_forwarded_for代理服务器地址
    :param count: 日志数量，默认生成100条
    """
    while count >= 1:
        nginx_log = "{ip} - - {local_time} \"{req_method} /{url} HTTP/1.1\" {status_code} {byte_size} \"{referer}\" \"{user_agent}\" \"{proxy_address}\"" \
            .format(local_time=random_htv_accessdatetime(),
                    req_method=random_http_reqmethod(),
                    url=random_htv_url(),
                    ip=random_htv_ip(),
                    status_code=random_htv_status_code(),
                    byte_size=random_byte_size(),
                    referer=random_htv_referer(),
                    user_agent=random_user_agent(),
                    proxy_address=random_htv_proxy_ip()
                    )
        print(nginx_log)
        FileUtils.writeDataToFile('./source_data', 'access-nginx', nginx_log)
        # FileUtils.writeDataToGZFile('./source_data', 'access-nginx', nginx_log)
        # linux env
        # FileUtils.writeDataToFile('/root/InsuranceUserProfile/source_data', 'access-nginx', nginx_log)
        count = count - 1


# def readDataFromNginLog():
#     file = open(f'./access-nginx-{time.strftime("%Y%m%d", time.localtime())}', 'r', encoding="utf-8")
#     return file.read()


if __name__ == '__main__':
    generate_nginx_log(5)
    # line = readDataFromNginLog()
    # print(line)
    # for line in FileUtils.readDataFromFile('./source_data', 'access-nginx'):
    #     print(line, end='')
