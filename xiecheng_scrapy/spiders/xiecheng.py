# -*- coding: utf-8 -*-
import scrapy
import re
import os
import math
import requests
import json
import jsonpath
import datetime
import redis
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy_redis.spiders import RedisCrawlSpider
from xiecheng_scrapy.items import XiechengScrapyItem ,XiechengTicket,XiechengComments,ScenicSourcePage,ScenicCommentPage
from xiecheng_scrapy.pipelines import old_com_page_dict,old_scenic_list ,old_comments_scenic_id
from fdfs_client.client import *
client_file = '/etc/fdfs/client.conf'
client = Fdfs_client(client_file)

class XiechengSpider(RedisCrawlSpider):
    name = 'xiecheng15'
    allowed_domains = ['www.ctrip.com','piao.ctrip.com']
    # start_urls = ['http://piao.ctrip.com/dest/u-_d6_d0_b9_fa/s-tickets/P1/']
    redis_key = 'xiecheng15:start_urls'

    rules = (
        Rule(LinkExtractor(allow=r'/dest/u-_d6_d0_b9_fa/s-tickets/P\d+'), callback='parse_item', follow=True),
    )

    # 获取景点url列表
    def parse_item(self,response):
        scenic_href = response.xpath('//a[@class="title"]/@href').extract()
        # print(scenic_href)
        scenic_list = []
        for href in scenic_href:
            scenic_url = 'http://piao.ctrip.com' + href
            scenic_list.append(scenic_url)
        # print(scenic_list)
        for url in scenic_list:
            yield scrapy.Request(url=url,callback=self.scenic_info,meta={'url':url})

    # 获取景点及其门票的信息
    def scenic_info(self,response):
        # 景点id
        url = response.meta['url']
        scenic_id = re.findall(r'.*\/t(\d+)',url)[0]
        print('scenic_id:',scenic_id)
        scenic_information = XiechengScrapyItem()
        # 景点基本信息抓取
        scenic_information['scenic_id'] = scenic_id
        scenic_name = response.xpath('//div[@class="brief-right"]/h2/text()').extract_first()
        # print('scenic_name',scenic_name)
        scenic_information['scenic_name'] = scenic_name
        
        # # 景点原网页写入文件系统
        scenic_source_page = ScenicSourcePage() 
        ret_upload = {}
        file = scenic_name+'.html'
        if scenic_id not in old_scenic_list:   #增量处理  
            with open(file,'w',encoding='utf-8') as fp:
                fp.write(response.text)
            ret_upload = client.upload_by_filename(file) 
            scenic_source_page['remote_file_id']=ret_upload['Remote file_id'] 
        if os.path.exists(file):
            #删除文件
            os.remove(file)

        # 景点网页信息写入数据库   
        scenic_source_page['scenic_id'] = scenic_id
        scenic_source_page['scenic_name'] = scenic_name
        scenic_source_page['source_url'] = url
        ##关系
        scenic_source_page['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
        print("写入景点原网页")
        yield scenic_source_page
        
        # 景点等级
        scenic_grade = response.xpath('//span[@class="spot-grade "]/strong/text()').extract_first()
        scenic_information['scenic_grade'] = scenic_grade
        # 景点地址
        scenic_address = response.xpath('//div[@class="brief-right"]/ul/li[1]/span/text()').extract_first()
        scenic_information['scenic_address'] = scenic_address
        # 景点开放时间
        scenic_time = response.xpath('//li[starts-with(@class,"time")]/span/text()').extract_first()
        scenic_information['scenic_time'] = scenic_time
        # 景点评分
        scenic_score = response.xpath('//i[@class="num"]/text()').extract_first() + '/5分'
        scenic_information['scenic_score'] = scenic_score
        # 景点价格
        # scenic_price = response.xpath('//div[@class="media-price"]/div/span/text()').extract_first().strip()
        # scenic_information['scenic_price'] = scenic_price
        # 景点交通
        scenic_traffic = response.xpath('//div[@class="traffic-content"]')  
        try: 
            scenic_tra = scenic_traffic.xpath('string(.)').extract_first().strip()
        except Exception: 
            scenic_tra = scenic_traffic.xpath('string(.)').extract_first()
        scenic_information['scenic_traffic'] = scenic_tra
        # 景点评论数
        sceinc_comments_count = response.xpath('//div[@class="score"]/a/text()').extract_first().split('看')[1].split('点')[0]
        scenic_information['scenic_comments_count'] = sceinc_comments_count
        # 景点政策
        scenic_polic = response.xpath('//dl[@class="notice-content"]')
        scenic_pol = scenic_polic.xpath('string(.)').extract_first().strip()
        scenic_information['scenic_polic'] = scenic_pol
        # 景区特色
        scenic_feature = response.xpath('//ul[@class="introduce-feature"]')
        scenic_fea = scenic_feature.xpath('string(.)').extract()
        scenic_information['scenic_feature'] = scenic_fea
        # 景区简介
        scenic_feature_content = response.xpath('//div[@class="introduce-content"]')
        scenic_feature_con = scenic_feature_content.xpath('string(.)').extract_first()
        scenic_information['scenic_feature_content'] = scenic_feature_con
        # 景区图片
        scenic_img = response.xpath('//div[@class="introduce-content"]/p/img/@src').extract()
        scenic_information['scenic_img'] = scenic_img
        scenic_information['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
        # print('景点基本信息：', scenic_information)
        # 景点信息写入
        print('开始写入景点信息')
        yield scenic_information

        # 若非第一次爬取，只爬取评论的前十页
        r = redis.Redis(host="10.0.0.97",port=6379,db=1)  
        if int(r.get("mark15").decode('utf-8')) > 1:
            if scenic_id in old_comments_scenic_id:
                if (int(sceinc_comments_count.split('条')[0])//10) + 1 > 100:
                    page = 10 
                else:
                    page = (int(sceinc_comments_count.split('条')[0])//10) + 1
            else:
                if int(sceinc_comments_count.split('条')[0]) < 3010:
                    #获取评论页数
                    page = (int(sceinc_comments_count.split('条')[0])//10) + 1
                else:
                    page = 302
        else:
            if int(sceinc_comments_count.split('条')[0]) < 3010:
                #获取评论页数
                page = (int(sceinc_comments_count.split('条')[0])//10) + 1
            else:
                page = 302
        #评论链接
        for i in range(1,page):
            print('评论抓取',i)
            commnet_url = 'http://sec-m.ctrip.com/restapi/soa2/12530/json/viewCommentList/'

            data = {"pageid":10650000804,"viewid":scenic_id,"tagid":-11,"pagenum":i,"pagesize":10,"contentType":"json","head":{"appid":"100013776","ctok":"","cver":"1.0","lang":"01","sid":"8888","syscode":"09","auth":"","extension":[]},"ver":"7.10.3.0319180000"}
            formdata=json.dumps(data) 
            yield scrapy.Request(url=commnet_url,method='POST',body=formdata,callback=self.get_comments,dont_filter=True,headers={'Content-Type':'application/json'},meta={'scenic_name':scenic_name,'scenic_id':scenic_id})         
        # 门票名称列表
        scenic_tickets_id = set(re.findall(r'\"resid\":(\d+)',response.text))  
        ticket_url = 'http://sec-m.ctrip.com/restapi/soa2/12530/json/resourceAddInfoQOC/'
        for ticket_id in scenic_tickets_id:
            data = {"pageid":10650000804,"resids":[ticket_id],"inchina":True,"thingsid":0,"thingspromid":0,"viewid":scenic_id,"contentType":"json","head":{"appid":"100013776","ctok":"","cver":"1.0","lang":"01","sid":"8888","syscode":"09","auth":"","extension":[]},"ver":"7.10.3.0319180000"}
            formdata=json.dumps(data) 
            yield scrapy.Request(url=ticket_url,method='POST',body=formdata,callback=self.get_tickets,dont_filter=True,headers={'Content-Type':'application/json'},meta={'scenic_name':scenic_name,'scenic_id':scenic_id,'ticket_id':ticket_id})  
    def get_tickets(self,response):
        scenic_name = response.meta['scenic_name']
        scenic_id = response.meta['scenic_id']
        ticket_id = response.meta['ticket_id']
        tickets_text = response.text  
        # print(tickets_text) 
        ticket_data = json.loads(tickets_text)
        # ticket_info = ticket_data['data']['ress']
        scenic_ticket = XiechengTicket()
        scenic_ticket['ticket_id'] = ticket_id
        scenic_ticket['scenic_id'] = scenic_id
        scenic_ticket['scenic_name'] = scenic_name
        scenic_ticket['ticket_data'] = ticket_data['data']
        scenic_ticket['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
        print('开始写入门票')
        yield scenic_ticket

    def get_comments(self,response):     
        scenic_name = response.meta['scenic_name']
        scenic_id = response.meta['scenic_id']
        comments_text = response.text
        # # 评论网页保存
        comment_page = ScenicCommentPage()      
        obj_comments = json.loads(comments_text)
        comment_data = obj_comments['data']        
        if comment_data["comments"] != [] :
            ret_upload = {}
            # 评论原网页写入文件系统
            comment_page_id = obj_comments['data']['comments'][0]['id']
            # 每页评论的  第一个评论的id=评论页面的id
            comment_page['comment_page_id'] = comment_page_id          
            file = scenic_name + str(comment_page_id) + '.json'          
            if scenic_id not in old_comments_scenic_id:
                with open(file,'w',encoding='utf-8') as fp:
                    fp.write(comments_text)
                ret_upload = client.upload_by_filename(file)     
            else:           
                if comment_page_id not in old_com_page_dict[scenic_id] and int(comment_page_id)>int(max(old_com_page_dict[scenic_id])):   #增量处理
                    with open(file,'w',encoding='utf-8') as fp:
                        fp.write(comments_text)
                    ret_upload = client.upload_by_filename(file) 
            comment_page['scenic_name'] = scenic_name    
            comment_page['remote_file_id'] = ret_upload['Remote file_id']
            comment_page['scenic_id'] = scenic_id           
            # comment_page['source_url'] = url     
            comment_page['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            if os.path.exists(file):
                #删除文件
                os.remove(file)
            print("写入评论原网页")           
            yield comment_page

        if comment_data["comments"] != [] :
            comments = comment_data["comments"]
            for comment in comments:
                # print(comment)
                scenic_comments = XiechengComments()
                scenic_comments['comment_id'] = comment['id']     
                scenic_comments['con'] = comment['content']
                scenic_comments['date'] = comment['date']
                scenic_comments['grade'] = comment['score']
                scenic_comments['uid'] = comment['uid']
                scenic_comments['scenic_id'] = scenic_id
                scenic_comments['scenic_name'] = scenic_name
                scenic_comments['mark'] = 0   # 分词标记
                scenic_comments['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
                print('开始写入评论')
                yield scenic_comments
