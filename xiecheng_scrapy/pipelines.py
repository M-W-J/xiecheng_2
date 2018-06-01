# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


import json
import redis
import pymongo
from scrapy.utils.project import get_project_settings
from xiecheng_scrapy.items import XiechengScrapyItem,XiechengComments,XiechengTicket,ScenicSourcePage,ScenicCommentPage

settings = get_project_settings()
r = redis.Redis(host="10.0.0.97",port=6379,db=1)
mark = int(r.get("mark15").decode('utf-8'))


if mark > settings['SYS_COUNT']:
    from xiecheng_scrapy.increment import comment_add,com_page_add,scenic_add,scenic_pickets_add,old_comments_scenic_id,old_tickets_scenic_id

    old_comments_dict = comment_add()
    old_com_page_dict = com_page_add()
    old_scenic_list = scenic_add()
    old_piclet_dict = scenic_pickets_add()
else:
    old_com_page_dict = {}
    old_scenic_list = []
    old_comments_scenic_id = []


class XiechengScrapyPipeline(object):
    def __init__(self):
        # settings = get_project_settings()
        # 链接数据库
        self.client = pymongo.MongoClient(host=settings['MONGO_HOST'], port=settings['MONGO_PORT'])
        
        self.db = self.client[settings['MONGO_DB']]  
        self.scenic_comments_coll = self.db[settings['SCENIC_COMMENTS_COLL']] 
        self.scenic_info_coll = self.db[settings['SCENIC_INFO_COLL']]  
        self.scenic_tickes_coll = self.db[settings['SCENIC_TICKES_COLL']] 
        self.scenic_page_coll = self.db[settings['SCENIC_PAGE_COLL']]
        self.comment_page_coll = self.db[settings['COMMENT_PAGE_COLL']]

    def process_item(self,item ,spider):  
        if mark > settings['SYS_COUNT']:
            print('这是增量爬取')                  
            # 评论新增
            if isinstance(item, XiechengComments):
                if dict(item)['scenic_id'] not in  old_comments_scenic_id:
                    print('********评论库中无此景点********',dict(item)['scenic_id'])
                    self.scenic_comments_coll.insert(dict(item))
                else:
                    # scenic_id = dict(item)['scenic_id']
                    if dict(item)['comment_id'] not in old_comments_dict[dict(item)['scenic_id']] and int(dict(item)['comment_id'])>int(max(old_comments_dict[dict(item)['scenic_id']])):
                        print('********评论库中无此评论********',dict(item)['comment_id'])
                        self.scenic_comments_coll.insert(dict(item))
            # 评论原页面新增
            if isinstance(item,ScenicCommentPage):
                if dict(item)['scenic_id'] not in old_comments_scenic_id:
                    print('********评论网页库无此景点********',dict(item)['scenic_id'])
                    self.comment_page_coll.insert(dict(item))
                else:
                    # scenic_id = dict(item)['scenic_id']
                    if dict(item)['comment_page_id'] not in old_com_page_dict[dict(item)['scenic_id']] and int(dict(item)['comment_page_id'])>int(max(old_com_page_dict[dict(item)['scenic_id']])):
                        print('********评论网页库无此网页********',dict(item)['comment_page_id'])
                        self.comment_page_coll.insert(dict(item))

            # 景点新增
            if isinstance(item, XiechengScrapyItem): 
                if dict(item)['scenic_id'] not in old_scenic_list:
                    print('********新增景点********',dict(item)['scenic_id'])
                    self.scenic_info_coll.insert(dict(item))
                else:
                    print('该景点已存在')
            # 景点原网页新增
            if isinstance(item,ScenicSourcePage):
                if dict(item)['scenic_id'] not in old_scenic_list:
                    print('********新增景点网页********',dict(item)['scenic_id'])
                    self.scenic_page_coll.insert(dict(item))
                else:
                    print('该景点已存在')
            #门票新增
            if isinstance(item, XiechengTicket): 
                if dict(item)['scenic_id'] not in old_tickets_scenic_id:
                    print('********门票库中无此景点********',dict(item)['scenic_id'])
                    self.scenic_tickes_coll.insert(dict(item))
                else:
                    # scenic_id = dict(item)['scenic_id']
                    if dict(item)['ticket_id'] not in old_piclet_dict[dict(item)['scenic_id']]:
                        print('********门票库中无此门票********',dict(item)['ticket_id'])
                        self.scenic_tickes_coll.insert(dict(item))
        else:
            if isinstance(item, XiechengComments):
                self.scenic_comments_coll.insert(dict(item))
            if isinstance(item, XiechengScrapyItem): 
                self.scenic_info_coll.insert(dict(item))  
            if isinstance(item, XiechengTicket): 
                self.scenic_tickes_coll.insert(dict(item))
            if isinstance(item,ScenicSourcePage):
                self.scenic_page_coll.insert(dict(item))
            if isinstance(item,ScenicCommentPage):
                self.comment_page_coll.insert(dict(item)) 

