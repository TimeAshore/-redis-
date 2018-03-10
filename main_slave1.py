#encoding: utf-8
import requests
import time
from lxml import etree
from redis import Redis
identity = 'slave1'

# master抓取url，存入redis
def get_urls():
    r = Redis(host='39.106.155.194', port=6379)
    print u'redis数据库：',r.keys('*')
    res = requests.get('https://www.doutula.com/').text
    res = etree.HTML(res)
    urls = res.xpath('//*[@id="home"]/div/div[2]/div[3]/ul/li/div[2]/div//a')
    for x in urls:
        url = x.xpath('@href')[0]
        print 'url:',url
        r.lpush('DouTu', url)
    print u'获取URL成功，当前redis长度为：',r.llen('DouTu')

# slave从redis中读url下载图片
def download_url():
    ans = 1
    # 连接远程服务器redis
    r = Redis(host='39.106.155.194', port=6379)
    print '开始前redis长度为：',r.llen('DouTu')   # 打印开始前redis中url的个数
    while ans:
        try:
            url = r.lpop('DouTu')  # 从redis中取出一个，取出以后redis中将会删除该url
            if url != None:
                print u'url:',url,u'剩余{}个'.format(r.llen('DouTu'))
                try:
                    content = requests.get(etree.HTML(requests.get(url).text).xpath('//*[@id="detail"]/div[1]/div[2]/li/div[3]/div/div/div/div[1]/table/tbody/tr[1]/td/img/@src')[0]).content
                except:
                    continue
                name = time.time()
                with open('./pic/' + identity + str(name) + '.jpg', 'wb') as f:   # 保存图片
                    f.write(content)
            else:
                raise Exception('url已读完')   # 当redis中url被取完后抛出异常退出
            ans += 1
            time.sleep(1)
        except Exception, e:
            print u'错误信息：',e
            break
    print '开始后redis长度为：',r.llen('DouTu')

if __name__ == '__main__':
    # 判断身份，master添加url后爬取，slave直接爬
    if identity == 'master':
        get_urls()
    else:
        download_url()

