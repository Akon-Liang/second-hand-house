import csv
import requests
import re
from lxml import etree
from fake_useragent import UserAgent as ua
import time
from selenium import webdriver
from threading import Thread
from queue import Queue
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities



def getiplist():
    proxieslist = [{'http': 'http://123.206.201.35:8118'},{'http': 'http://112.95.224.58:8118'}, {'http': 'http://112.95.224.58:8118'}, \
                   {'http': 'http://106.14.11.65:8118'}, {'http': 'http://39.108.168.155:8118'}, {'http': 'http://120.78.179.218:8118'},\
                   {'http': 'http://115.29.170.58:8118'}, {'http': 'http://115.29.170.58:8118'}, {'http': 'http://118.24.3.29:8118'},\
                   {'http': 'http://118.24.3.29:8118'}, {'http': 'http://180.168.13.26:8000'}, {'http': 'http://121.232.199.131:9999'},\
                   {'http': 'http://39.107.84.185:8123'}, {'http': 'http://120.26.127.90:8118'}, {'http': 'http://120.77.201.46:8080'},\
                   {'http': 'http://47.94.104.204:8118'}, {'http': 'http://47.105.168.201:8118'}, {'http': 'http://139.199.38.177:8118'}, \
                   {'http': 'http://119.29.224.144:8118'}, {'http': 'http://39.107.84.185:8123'}, {'http': 'http://39.107.84.185:8123'},\
                   {'http': 'http://139.196.76.27:8118'}, {'http': 'http://182.92.113.148:8118'}, {'http': 'http://101.200.50.18:8118'},\
                   {'http': 'http://101.200.50.18:8118'}, {'http': 'http://101.200.50.18:8118'}, {'http': 'http://47.94.104.204:8118'}, \
                   {'http': 'http://118.24.156.214:8118'}, {'http': 'http://39.107.76.192:8118'}, {'http': 'http://120.198.230.65:8080'},\
                   {'http': 'http://120.79.7.88:8118'}, {'http': 'http://182.92.233.137:8118'},{'http': 'http://111.230.254.195:8118'}, \
                   {'http': 'http://182.92.113.183:8118'}, {'http': 'http://139.199.38.177:8118'}, {'http': 'http://114.234.83.93:9000'}, \
                   {'http': 'http://47.96.148.248:8118'}, {'http': 'http://139.196.76.27:8118'}, {'http': 'http://101.200.50.18:8118'}, \
                   {'http': 'http://119.29.26.242:8080'}, {'http': 'http://180.168.13.26:8000'}, {'http': 'http://180.118.86.3:9000'}, \
                   {'http': 'http://182.92.233.137:8118'}, {'http': 'http://139.199.117.41:8118'}, {'http': 'http://106.14.206.26:8118'},\
                   {'http': 'http://101.200.50.18:8118'}, {'http': 'http://115.29.170.58:8118'}, {'http': 'http://180.168.13.26:8000'}, \
                   {'http': 'http://114.235.23.69:9000'}, {'http': 'http://180.168.13.26:8000'}, {'http': 'http://114.234.81.209:9000'},\
                   {'http': 'http://47.96.136.190:8118'}, {'http': 'http://114.234.83.42:9000'}, {'http': 'http://117.87.177.219:9000'},\
                   {'http': 'http://114.234.82.183:9000'}, {'http': 'http://180.168.13.26:8000'}, {'http': 'http://112.95.224.58:8118'},\
                   {'http': 'http://139.196.51.201:8118'}, {'http': 'http://114.234.81.209:9000'}, {'http': 'http://106.14.197.219:8118'},\
                   {'http': 'http://180.118.86.3:9000'}, {'http': 'http://183.154.212.165:9000'}, {'http': 'http://114.235.23.157:9000'},\
                   {'http': 'http://180.104.62.233:9000'}, {'http': 'http://117.87.176.171:9000'}, {'http': 'http://114.234.80.233:9000'},\
                   {'http': 'http://114.235.22.101:9000'}, {'http': 'http://120.77.201.46:8080'}, {'http': 'http://114.234.83.46:9000'},\
                   {'http': 'http://114.234.83.46:9000'}, {'http': 'http://120.26.199.103:8118'}, {'http': 'http://218.75.70.3:8118'},\
                   {'http': 'http://120.26.199.103:8118'}, {'http': 'http://180.118.247.192:9000'}, {'http': 'http://115.29.170.58:8118'}, \
                   {'http': 'http://119.23.35.209:9000'}, {'http': 'http://115.29.170.58:8118'}, {'http': 'http://115.29.170.58:8118'},\
                   {'http': 'http://115.29.170.58:8118'}, {'http': 'http://115.29.170.58:8118'}, {'http': 'http://115.29.170.58:8118'}, \
                   {'http': 'http://117.90.3.141:9000'}, {'http': 'http://124.127.255.27:8080'}, {'http': 'http://124.127.255.27:8080'},\
                   {'http': 'http://123.57.61.38:8118'}, {'http': 'http://182.92.113.183:8118'}, {'http': 'http://182.92.233.137:8118'}, \
                   {'http': 'http://112.17.250.78:8080'}, {'http': 'http://117.90.3.141:9000'}, {'http': 'http://115.29.170.58:8118'}, \
                   {'http': 'http://115.29.170.58:8118'}, {'http': 'http://123.127.8.248:8080'}, {'http': 'http://123.127.8.248:8080'},\
                   {'http': 'http://115.29.170.58:8118'}, {'http': 'http://183.61.71.112:8888'}, {'http': 'http://115.29.170.58:8118'}, \
                   {'http': 'http://112.17.250.78:8080'}, {'http': 'http://112.17.250.78:8080'}, {'http': 'http://115.29.170.58:8118'}, \
                   {'http': 'http://114.235.22.239:9000'}, {'http': 'http://180.104.62.130:9000'}, {'http': 'http://114.234.82.206:9000'},\
                   {'http': 'http://101.64.182.2:8080'}, {'http': 'http://120.132.52.88:8888'}, {'http': 'http://120.132.52.88:8888'}, \
                   {'http': 'http://120.132.52.88:8888'}, {'http': 'http://120.132.52.88:8888'}, {'http': 'http://110.19.156.112:8090'}, \
                   {'http': 'http://182.90.54.199:8090'}, {'http': 'http://115.29.97.12:8080'}, {'http': 'http://115.29.97.12:8080'},\
                   {'http': 'http://1.85.2.253:8001'}, {'http': 'http://36.230.84.143:8088'}]
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0'}
    url = 'https://gz.lianjia.com/'
    print('ip列表生成中，约需要3分钟，请稍后......')
    for i in  proxieslist:
        try:
            res = requests.get(url,headers = headers,proxies = i,timeout=2)
            if res.status_code == 200:
                raise
        except:
            proxieslist.remove(i)
    print('ip列表生成完毕，有效ip共有%s条'%len(proxieslist))
    return proxieslist
iplist = getiplist()
urllist = ['aotouzhen/', 'baijiang/', 'baiyun/', 'baiyundadaonan/', 'baogang/', 'beijinglu/', 'beixingzhen/', 'binhaihuayuan/', 'binjiangxi/', 'binjiangzhong/', 'cencun/', 'chajiao/', 'changgang1/', 'changlong/', 'changxing1/', 'changzhoudao1/', 'chebei/', 'chenjiaci/', 'chigang/', 'chinizhen/', 'dagangzhen/', 'dajinzhonglu/', 'dashadi/', 'dashi/', 'datansha/', 'daxuecheng/', 'dongchuanlu/', 'dongfengdong/', 'dongfengxi/', 'donghu1/', 'donglang/', 'dongping1/', 'dongpu/', 'dongshankou/', 'dongxiaolu/', 'dongxiaonan/', 'dongyixinqu/', 'dongyong/', 'ershadao/', 'fangcun/', 'fenghuangcheng/', 'fenshui/', 'fuhaoshanzhuang/', 'fuhezhen/', 'gaotang/', 'gongyedadaobei/', 'gongyedadaonan/', 'gongyedadaozhong/', 'guanggangxincheng/', 'guangzhoudadaonan/', 'guangzhounanzhan/', 'guanzhou/', 'guihuagang/', 'haizhu/', 'haizhuguangchang/', 'hanxi/', 'hebinbeilu/', 'hedong1/', 'hepingxi/', 'hongde/', 'huadiwan/', 'huadongzhen/', 'huajingxincheng/', 'hualong/', 'huananbiguiyuan/', 'huananxincheng/', 'huangbian/', 'huangcun/', 'huangge/', 'huanghuagang/', 'huangpucun/', 'huangpuqufu/', 'huangsha/', 'huangshi/', 'huanshidadaoxi/', 'huanshidong/', 'huashanzhen/', 'huazhou/', 'huijiang/', 'huijingxincheng/', 'jiahewanggang/', 'jianggaozhen/', 'jiangnandadaozhong/', 'jiangnanxi/', 'jiangpujie/', 'jiangxia1/', 'jiangyanlu/', 'jianshelu1/', 'jiaokou1/', 'jichanglu/', 'jiefangbei/', 'jiefangnan/', 'jinbi/', 'jingangdadao/', 'jinghudadao/', 'jingtai/', 'jingxi1/', 'jinrongcheng1/', 'jinshangu/', 'jinshazhou/', 'jinzhou2/', 'jiuchengqu/', 'jiuqu/', 'jushu/', 'kaifadongqu/', 'kaifaxiqu/', 'kecun/', 'kengkou/', 'kexuecheng/', 'lanhezhen/', 'liangkouzhen/', 'liangtianzhen/', 'lianhuashan/', 'lichengfupeng/', 'lichengxiqu/', 'lichengzengjiang/', 'lichengzhongqu/', 'lijiao/', 'liuhuazhanqian/', 'longdong/', 'longjin/', 'longkoudong/', 'longkouxi/', 'longxi1/', 'lujing/', 'luochongwei/', 'luoxi/', 'mawu/', 'meihuayuan/', 'modiesha/', 'nananlu/', 'nancun/', 'nanhu5/', 'nanpu/', 'nanshagang/', 'nanshajiuzhen/', 'nanshakejiyuan/', 'nanshaqufu/', 'nanzhou/', 'nongjiangsuo/', 'paitanzhen/', 'panfu/', 'panyuguangchang/', 'panyukeyunzhan/', 'pazhou/', 'qianjinlu/', 'qiaonan4/', 'qifuxincun/', 'renhe1/', 'renminbei1/', 'renminlu/', 'sanyuanli/', 'shacun/', 'shahe1/', 'shamian/', 'shanqiandadao/', 'shataibei/', 'shatainan/', 'shawan1/', 'shayuan/', 'shengangzhen/', 'shenshanzhen/', 'shiji1/', 'shijing/', 'shilingzhen/', 'shilou/', 'shipai1/', 'shiqiao1/', 'shiqiaobei/', 'shiqiaodong/', 'shitanzhen/', 'shuiyin/', 'shundebiguiyuan/', 'taihe/', 'taipingzhen/', 'tanbu/', 'tangxia1/', 'taojin/', 'tianhe/', 'tianhegongyuan/', 'tianhekeyunzhan/', 'tianhenan/', 'tianrunlu/', 'tieluxi/', 'tiyuzhongxin/', 'tongdewei/', 'tongfu/', 'tonghe1/', 'wanbo/', 'wangchengpianqu/', 'wanqingsha/', 'wanshengwei/', 'wenchong/', 'wenquanzhen/', 'wushan/', 'wuyangxincheng/', 'xiajiao/', 'xiamao/', 'xiangxue/', 'xiaobei/', 'xiaolouzhen/', 'xiayuan1/', 'xichang/', 'xicun/', 'xiguan/', 'xihualu/', 'xilang/', 'ximenkou/', 'xinchengpianqu/', 'xingangxi/', 'xinghewan/', 'xinqu1/', 'xintangbei/', 'xintangnan/', 'xinzao/', 'yajule1/', 'yangji/', 'yantang/', 'yayuncheng/', 'yayundadaozhong/', 'yingzhou1/', 'yonghe1/', 'yongtai/', 'yuancun/', 'yuanxiatian/', 'yueken/', 'yuexiu/', 'yuexiunan/', 'yuwotou/', 'yuzhu/', 'zengchengbiguiyuan/', 'zengchengqufu/', 'zhengguozhen/', 'zhishicheng/', 'zhongcun/', 'zhongda/', 'zhongluotan/', 'zhongshanba1/', 'zhongxinzhen/', 'zhoumen/', 'zhucun/', 'zhujiangxinchengdong/', 'zhujiangxinchengxi/', 'zhujiangxinchengzhong/']


class lianjiaspider(object):
    def __init__(self,urllist,iplist):
        self.ipQueue = Queue()
        for i in iplist:
            self.ipQueue.put(i)
        self.urlQueue = Queue()
        for k in urllist:
            url = 'https://gz.lianjia.com/chengjiao/' + k
            self.urlQueue.put(url)
        self.urlQueue1 = Queue()
        self.htmlQueue = Queue()

    def geturl1(self):
        print('5')
        ip = self.ipQueue.get()
        while 1:
            try:
                url = self.urlQueue.get(block=False,timeout=10)
                res0 = requests.get(url, headers={"User-Agent":str(ua().random)}, proxies=ip)
                res0.encoding = 'utf-8'
                html = res0.text
                q = re.compile(r'https://gz.lianjia.com/chengjiao/.*?[.]html')
                r = '//div[@class="total fl"]/span/text()'

                parseHtml0 = etree.HTML(html)
                datab = parseHtml0.xpath(r)
                pn = int(datab[0])
                if pn == 0:
                    continue
                pageend = pn // 30 + 2
                for k in range(1, pageend):
                    if k == 1:
                        pass
                    else:
                        urlbase1 = url + 'pg%s/' % k
                        res0 = requests.get(urlbase1, headers={"User-Agent":str(ua().random)}, proxies=ip)
                        res0.encoding = 'utf-8'
                        html = res0.text
                    dataa = q.findall(html)
                    dataa = set(dataa)
                    dataa = list(dataa)
                    for km in dataa:
                        self.urlQueue1.put(km)


            except:
                break

    def configdriver(self):
        print('2')
        ip = self.ipQueue.get()
        dcap = dict(DesiredCapabilities.PHANTOMJS)
        dcap["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1"
        # driver = webdriver.PhantomJS(desired_capabilities=dcap)

        service_args = [
            '--proxy=%s' % ip,  # 代理 IP：prot  （eg：192.168.0.28:808）
            '--proxy-type=http',  # 代理类型：http/https
            '--load-images=no',  # 关闭图片加载（可选）
            '--disk-cache=yes',  # 开启缓存（可选）
            '--ignore-ssl-errors=true'  # 忽略https错误（可选）
        ]
        driver = webdriver.PhantomJS(service_args=service_args, desired_capabilities=dcap)
        return driver

    def getHtml(self):
        print('3')


        def mysleep(driver):
            while True:
                try:
                    try:
                        driver.find_element_by_css_selector('#mapListContainer > div')
                        a = False
                        return a
                    except:
                        driver.find_element_by_css_selector('#mapListContainer > ul > li:nth-child(1) > div')
                        a = True
                    return a
                except:
                    time.sleep(0.1)
                    continue


        driver = self.configdriver()

        while True:

            try:
                htmllist = []
                zm = self.urlQueue1.get(block=False,timeout=20)
                driver.get(zm)
                js = "var q=document.documentElement.scrollTop=10000"
                driver.execute_script(js)

                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist":a,"html":html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > ul > li:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(3)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(4)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > ul > li:nth-child(3)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > ul > li:nth-child(4)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(3)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > ul > li:nth-child(5)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(3)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(4)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > ul > li:nth-child(6)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(2)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(3)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                driver.find_element_by_css_selector('#around > div > div.tabBox > div.itemTagBox > div:nth-child(4)').click()
                a = mysleep(driver)
                html = driver.page_source
                htmllist.append({"isexist": a, "html": html})
                time.sleep(0.5)
                self.htmlQueue.put(htmllist)
            except:
                break
        driver.quit()


    def parseHtml(self):
        print('1')
        while 1:
            try:
                htmllist = self.htmlQueue.get(block=True,timeout=70)
            except:
                break
            try:
                parseHtml = etree.HTML(htmllist[0]['html'])
                # 地区
                data0 = parseHtml.xpath('//div[@class="name"][1]//a/text()')
                # 小区名、户型、面积
                data = parseHtml.xpath('//h1[@class="index_h1"]/text()')
                data = data[0].split(' ')
                data = data[:-1]
                # 成交时间
                data1 = parseHtml.xpath('//div[@class="wrapper"]/span/text()')
                data1 = data1[0].split(' ')
                data1 = [data1[0]]
                # 成交价格(万)
                data2 = parseHtml.xpath('//span[@class="dealTotalPrice"]/i/text()')
                # 成交价格(元/平方)
                data3 = parseHtml.xpath('//div[@class="price"]/b/text()')
                # 挂牌价格、成交周期、调价、带看、关注、浏览
                data4 = parseHtml.xpath('//div[@class="msg"]/span/label/text()')
                # 基本属性前14、交易属性后8
                data5 = parseHtml.xpath('//div[@class="content"]//li/text()')
                datax = []
                datax += data0
                datax += data
                datax += data1
                datax += data2
                datax += data3
                datax += data4
                datax += data5
                for i in htmllist:
                    if i['isexist']:
                        q = re.compile('itemText itemTitle">(.*?)</span>.*?itemdistance">(\d*米)</span>.*?itemInfo">(.*?)</div>')
                        data6 = q.findall(i['html'])
                    else:
                        data6 = '无相关周边配套'
                    data6 = str(data6)
                    datax.append(data6)
                with open('广州二手房成交信息.csv', 'a', encoding='gb18030') as m:
                    writer1 = csv.writer(m)
                    writer1.writerow(datax)
            except:
                continue



        print('解析模块已退出')

    def workOn(self):
        print('4')
        tlist = []
        for i in range(30):
            if i % 29 == 0:
                t = Thread(target=self.geturl1)
                t.start()
                tlist.append(t)
            if i%6 == 0:
                t1 = Thread(target=self.getHtml)
                t1.start()
                tlist.append(t1)
            if i%20 == 0:
                t2 = Thread(target=self.parseHtml)
                t2.start()
                tlist.append(t2)

        for i in tlist:
            i.join()



if __name__ == '__main__':
    spider = lianjiaspider(urllist,iplist)
    spider.workOn()
























