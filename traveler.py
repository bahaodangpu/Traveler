#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
Google Python Name Rule
module_name, package_name, ClassName, method_name,
ExceptionName, function_name, GLOBAL_VAR_NAME, instance_var_name,
function_parameter_name, local_var_name.
'''

import pymysql
import requests
from bs4 import BeautifulSoup
from  multiprocessing import Pool
import re,os,sys,time
import logging.config

'''
加载日志的配置文件，配置文件必须在脚本的同级目录下
'''

if not os.path.exists('logs'):
    os.mkdir('logs')
logging.config.fileConfig("logging.conf")
log_info = logging.getLogger('infoLogger')
log_error = logging.getLogger('errorLogger')
'''
    by bahao
    从一个网站出发，获取其所有链接，并与数据库中已有的链接判断是否重复，
    如果重复，则在这条边上增加一个权重，如果不重复则增加一个点并增加一条边设置权重为1.
'''
class TDataBase:

    def __init__(self,host='127.0.0.1',user='root',passwd='d956980781'):
        self.pid = 'PID '+str(os.getpid())+': '
        log_info.info(self.pid+'正在建立数据库连接...')
        self.host = host
        self.user = user
        self.passwd = passwd
        #创建与数据库的连接
        conn = pymysql.connect(host=host, user=user, passwd=passwd)
        cur = conn.cursor()
        if conn!=None:
            self.conn = conn
            log_info.info(self.pid+'建立数据库连接conn成功！')
        if cur!=None:
            self.cur = cur
            log_info.info(self.pid+'建立数据库游标cur成功！')

    '''
    创建数据库和数据表
    '''
    def create_db(self):
        try:
            self.cur.execute('CREATE DATABASE IF NOT EXISTS traveler CHARACTER SET UTF8')
            self.cur.execute('USE traveler')
            self.cur.execute('CREATE TABLE IF NOT EXISTS sites (id BIGINT(7) NOT NULL AUTO_INCREMENT,site VARCHAR(100) NOT NULL unique,looked CHAR(1) NOT NULL DEFAULT \'F\',PRIMARY KEY (id))')
            self.cur.execute('CREATE TABLE IF NOT EXISTS edges (id BIGINT(7) NOT NULL AUTO_INCREMENT,fromsite VARCHAR(100) NOT NULL,tosite VARCHAR(100) NOT NULL,weight BIGINT(7) NOT NULL DEFAULT 0,PRIMARY KEY (id))')
        except BaseException as e:
            #todo:pymysql执行SQL语句的时候到底返回的是什么？，然后在日志上打印出来。
            log_info.error(self.pid+'创建数据库的时候'+str(e))
            # print('Error def __init__(self,host,user,passwd):')

    def use_db(self):
        self.cur.execute('USE traveler')
        log_info.info(self.pid+'USE traveler')

    def get_sites(self, start=0, num=-1):
        '''

        :param start: 从数据库的前start条数据开始选取
        :param num:选取num条数据
        :return:返回一个List 其中的数据为查询到的站点 example
        '''
        #todo:举一个例子说明返回值的样子

        cmd = 'SELECT site FROM sites'
        if num == -1:
            self.cur.execute('SELECT site FROM sites')
        else:
            cmd = cmd+' limit '+start+' ,'+num
            try:
                self.cur.execute(cmd)
            except BaseException as e:
                log_info.error(self.pid+e)
        result = self.cur.fetchall()
        return result

    def get_sites_notlooked(self, num):
        '''
        获取指定的num条没有被爬取过得网址
        :param num:指定获取网址的数量
        :return:一个网址的列表 example：
        '''
        #todo:举例返回的数据类型
        log_info.info(self.pid+'getSitesNotLooked '+str(num))
        if num < 1:
            log_info.info(self.pid+'getSitesNotLooked : num < 1')
            return None
        cmd_get = 'SELECT site FROM sites WHERE looked = \'F\' LIMIT '+str(num)
        if self.cur!= None:
            try:
                self.cur.execute(cmd_get)
            except BaseException as e1:
                log_info.error(self.pid+e1)
                # print('ERROR：in ！getSitesNotLooked')
            result = self.cur.fetchall()
        else:
            log_info.info(self.pid+'getSitesNotLooked cur is None')
        sites = list()
        for item in result:
            sites.append(item[0])
        cmd_set_T = 'UPDATE sites SET looked=\'T\' WHERE site in '+str(tuple(sites))
        if len(sites) == 1:
            cmd_set_T = cmd_set_T[:-2]+')'
        #print('cmd_set_T is : ',cmd_set_T)
        try:
            log_info.info('cmd_set_t is %s ' % cmd_set_T)
            self.cur.execute(cmd_set_T)
            self.conn.commit()
        except BaseException as e2:
            log_info.error(self.pid+e2)
        if result!=None:
            return sites
        return None

    def get_edges(self, start = 0, num = -1):
        '''
        返回特定的边的数量
        :param start:
        :param num:
        :return:返回一个列表 example：
        '''
        #todo:举例返回的数据类型
        cmd = 'SELECT fromsite,tosite,weight FROM edges '
        if num == -1:
            self.cur.execute(cmd)
        else:
            cmd = cmd + start+' ,'+num
            try:
                self.cur.execute(cmd)
            except BaseException as e:
                log_info.error(self.pid+e)
        result = self.cur.fetchall()
        return result

    '''
    将一个tuple的站点列表插入到数据库中
    '''
    def write_sites(self, sites):
        '''
        将一个List的站点列表插入到数据库中
        :param sites:list数组 example：
        :return:没有返回值
        '''
        #todo:举例说明参数的类型
        if self.cur == None:
            log_info.error(self.pid+'writeSites cur is None')
            return 'WRONG!'
        cmd = 'INSERT IGNORE INTO sites (site) VALUES '
        if len(sites)<1:
            return
        cmd = cmd +'(\''+ str(sites[0])+'\')'
        for item in sites[1:]:
            cmd  = cmd + ',(\''+str(item)+'\')'
        #print('cmd = ',cmd)
        try:
            self.cur.execute(cmd)
        except :
            pass
        self.conn.commit()

    def write_onesite(self, site):
        '''
        向数据库中写入一条记录
        :param site:字符串 example: http://www.example.com
        :return:None
        '''
        if self.cur == None:
            log_info.error(self.pid+'write_onesite cur is None')
            return
        cmd = 'INSERT IGNORE INTO sites (site) VALUES '+'(\''+site+'\')'
        try:
            self.cur.execute(cmd)
        except BaseException as e:
            log_info.error(self.pid+e)
            # print('ERROR ! write data error in writeOneSite')
        self.conn.commit()

    def write_edges(self, edges):
        '''
        将一个List写入数据库
        :param edges: 一个List数组写入数据库 example：[(fromsite,tosite,weight),]
        :return:
        '''
        if self.cur == None:
            log_info.error(self.pid+'write_edges cur is None')
            return
        cmd = 'INSERT INTO edges (fromsite,tosite,weight) VALUES '
        if len(edges)<1:
            log_info.info(self.pid+'in write_edges len of edges  < 1')
            return
        cmd = cmd+str(edges[0])
        for item in edges[1:]:
            cmd = cmd +','+str(item)
        try:
            self.cur.execute(cmd)
        except BaseException as e:
            log_info.error(self.pid+e)
        self.conn.commit()

    def release(self):
        log_info.info(self.pid+'release database')
        self.cur.close()
        self.conn.close()

class Traveler():
    def __init__(self,start_url):
        self.pid = 'PID ' + str(os.getpid()) + ': '
        log_info.info(self.pid+'init Traveler in %s' % (start_url))
        self.start_url = start_url
        self.all_urls = []
        self.count = 0
        self.headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}


    def initailze(self):
        '''
        初始化数据库，获取数据库中的sites，edges
        获取requests的Session
        '''
        log_info.info(self.pid+'in Traveler initailze create db and connect db')
        self.db = TDataBase()
        self.db.use_db()
        self.sites = self.db.get_sites()
        self.edges = self.db.get_edges()
        self.session = requests.Session()

    def get_inout_urls_fromsite(self, site):
        '''
        获取一个网址上的所有内联和外链
        :param site:  网址 example : http://www.example.com
        :return: 以列表形式返回两个值 in_sites,out_sites
        '''

        log_info.info(self.pid+'in Traveler get_inout_urls_fromsite : '+str(site))
        domin = self.get_domin(site)
        in_sites = set()
        out_sites = set()
        try:
            response = self.session.get(site,headers = self.headers,timeout = 10)
            log_info.info(self.pid+'In Traveler get_inout_urls_fromsite : '+site)
        except BaseException as e:
            log_info.error(self.pid+e)
            # print('ERROR net error getInOutUrlsFromSite')
            return
        soup = BeautifulSoup(response.text.encode().decode(),'lxml')
        if soup == None:
            log_info.info(self.pid+'In Traveler get_inout_urls_fromsite : soup is None')
            return
        for item in soup.find_all('a'):
            link = item.get('href')
            if link==None:
                continue
            if link.startswith('http') and link.startswith(domin):
                in_sites.add(link)
            elif link.startswith('http') and not link.startswith(domin):
                out_sites.add(link)
                #print('domin is : ',self.getDomin(link))
        return in_sites,out_sites

    def get_all_out_urls(self, site):
        '''
        获取一个网址的所有外链
        :param site: 网址 example: http://www.example.com
        :return:列表 out_urls
        '''

        urls = set()
        urls_looked=set()
        urls.add(site)
        out_urls = set()
        #print("urls is :",urls)
        while True:
            if len(urls)>0:
                link = urls.pop()
                result_in,result_out = self.get_inout_urls_fromsite(link)
                urls_looked.add(link)
            else:
                log_info.info(self.pid+'In Traveler get_all_out_urls len of urls <= 0 ')
                return out_urls
            if result_in!=None:
                result_in = result_in.difference(urls_looked)
                urls = urls.union(result_in)
                #print('len of urls is :',len(urls))
                #print('len of urls_looked is :',len(urls_looked))
            if result_out!=None:
                out_urls = out_urls.union(result_out)
                #print('len of out_urls is :',len(out_urls))
        return out_urls

    def get_domin(self, url):
        '''
        获取一个网址的com之前得部分
        :param url: example 1 : http(s)://www.example.com?123456 ;example 2 : http(s)://www.example.com/123456
        :return:返回com/cn/net/...之前得部分 example : http://www.example.com
        '''

        if not url.startswith('http'):
            log_info.info(self.pid+'In Travel get_domin url not starte with http')
            return
        tmp = url.split('/')
        if len(tmp) < 3:
            return
        return re.match('^http(s)?:\/\/(.*?)(\/|\?)',url+'/').group()[:-1]

    def make_outurls(self, domin, urls):
        '''
        生成能够存入数据库的sites和edges
        :param domin: fromsite example http://www.example.com
        :param urls: 外链 example ： http://www.example.com
        :return:
        '''
        #建立一个表示边与权重的tuple(fromSite,toSite,weight)
        log_info.info(self.pid+'In Traveler make_outurls ')
        d = dict()
        t = list()
        s = list()
        for url in urls:
            site = self.get_domin(url)
            if site ==None:
                continue
            if site in d.keys():
                d[site] = d[site]+1
            else:
                d[site] = 1
        for key in d:
            t.append((domin,key,d[key]))
            s.append(key)
        return s,t

    def save(self,sites,edges):
        '''
        保存网址和边
        :param sites:
        :param edges:
        :return:
        '''

        self.db.write_edges(edges)
        self.db.write_sites(sites)
        self.db.release()

    def crawl(self,name = 'DefaultName'):
        '''
        爬虫的入口函数，开始爬取网页
        :param name:爬虫的名字
        :return:
        '''
        log_info.info(self.pid+'Run Task %s (%s) url is (%s)'% (name,os.getpid(),self.start_url))
        print('Run Task %s (%s) url is (%s)'% (name,os.getpid(),self.start_url))
        self.initailze()
        t_s = time.time()
        urls = self.get_all_out_urls(self.start_url)
        sites,edges = self.make_outurls(self.get_domin(self.start_url), urls)
        #print(edges)
        self.save(sites,edges)
        t_e = time.time()
        #print(len(edges))
        #print('traveler 耗时：', t_e - t_s)
        log_info.info(self.pid+'Task %s Runed %s秒 ,pid is %s' %(name,int(t_e - t_s),os.getpid()))
        print('Task %s Runed %s s ,pid is %s' %(name,int(t_e - t_s),os.getpid()))
#
# def testDB():
#     e = [('www.google2.com','www.youtube.com',1234),('www.google3.com','www.youtube.com',234)]
#     db = TDataBase('127.0.0.1','root','d956980781')
#     # db.writeEdges(e)
#     # edges = db.getEdges()
#     #site = db.getSitesNotLooked(10)
#     db.write_onesite('http://www.baidu.com')
#     db.release()
#     #print(site)
#
# def t():
#     t = Traveler('http://www.baidu.com')
#     t.run()

if __name__ == '__main__':
    print('start crawl ...')
    log_info.info('start crawl ...')
    db = TDataBase()
    db.create_db()
    if len(sys.argv) > 1:
        start_site = sys.argv[1]
        db.write_onesite(start_site)
    process_num = 0
    r = 0
    while True:
        r = r + 1
        print('start circle '+str(r))
        log_info.info('start circle '+str(r))
        sites = db.get_sites_notlooked(128)
        log_info.info('the sites in circle %s is %s' % (r,str(sites)))
        print('the sites in %s circle is %s' % (r,str(sites)))
        if len(sites) < 1:
            print('from database cannot read useful site')
            log_info.error('from database cannot read useful site')
            sys.exit()
        p = Pool(128)
        for i in range(len(sites)):
            little_spider = Traveler(sites.pop())
            # p.apply(little_spider.crawl,args=(process_num,))
            p.apply_async(little_spider.crawl,args=(('%s of circle %s' % (i,r)),))
        p.close()
        p.join()
        print('finish circle ' + str(r))
        log_info.info('finish circle ' + str(r))
    db.release()
    print('finish crawl')
    log_info.info('finish crawl')
