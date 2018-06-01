# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import redis

# 若非第一次爬取，只爬取评论的前十页
r = redis.Redis(host="10.0.0.97",port=6379,db=1)  
if r.get("mark15"):  
    r.set("mark15",int(r.get("mark15").decode('utf-8')) + 1)
    print("这是第{}次爬取".format(r.get("mark15").decode('utf-8')))
else:
    r.set("mark15",1)
    print("这是第{}次爬取".format(r.get("mark15").decode('utf-8')))
# 景点基本信息
class XiechengScrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    scenic_name = scrapy.Field()
    scenic_grade = scrapy.Field()
    scenic_address = scrapy.Field()
    scenic_time = scrapy.Field()
    scenic_score = scrapy.Field()
    scenic_price = scrapy.Field()
    scenic_traffic = scrapy.Field()
    scenic_comments_count = scrapy.Field()
    scenic_feature = scrapy.Field()
    scenic_feature_content = scrapy.Field()
    scenic_img = scrapy.Field()
    scenic_polic = scrapy.Field()
    scenic_id = scrapy.Field()
    crawl_time = scrapy.Field()

# 门票信息
class XiechengTicket(scrapy.Item):
    ticket_id = scrapy.Field()
    scenic_id = scrapy.Field()   
    ticket_data = scrapy.Field()
    scenic_name = scrapy.Field()
    crawl_time = scrapy.Field()

# 景点评论
class XiechengComments(scrapy.Item):
    comment_id = scrapy.Field()
    con = scrapy.Field()
    date = scrapy.Field()
    grade = scrapy.Field()
    uid = scrapy.Field()
    Reply = scrapy.Field()
    scenic_name = scrapy.Field()
    scenic_id = scrapy.Field()
    crawl_time = scrapy.Field()
    mark = scrapy.Field()

# 景点网页源码
class ScenicSourcePage(scrapy.Item):
    scenic_name = scrapy.Field()
    scenic_id = scrapy.Field()
    remote_file_id = scrapy.Field()
    source_url = scrapy.Field()
    crawl_time = scrapy.Field()

# 评论原网页
class ScenicCommentPage(scrapy.Item):
    scenic_name = scrapy.Field()
    scenic_id = scrapy.Field()
    remote_file_id = scrapy.Field()
    source_url = scrapy.Field()
    comment_page_id = scrapy.Field()
    crawl_time = scrapy.Field()
    


