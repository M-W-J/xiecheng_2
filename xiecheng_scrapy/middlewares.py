# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import redis
from random import choice
r = redis.Redis(host="10.0.0.97",port=6379,db=2,charset='utf-8')

class Proxy():
    def process_request(self, request, spider): 
        ip_list = self.get_proxy()        
        proxy = choice(ip_list)
        print(proxy)
        request.meta['proxy'] = proxy
        return None
    def process_response(self, request, response, spider):
        if response.status != 200:  
            # 对当前request加上代理  
            ip_list = self.get_proxy()        
            proxy = choice(ip_list)
            print("换代理",proxy)
            request.meta['proxy'] = proxy
            return request  
        return response  
    def get_proxy(self):
        ip01 = str(r.get("daili:list:1").decode('utf-8'))
        ip02 = str(r.get("daili:list:2").decode('utf-8'))
        redis_list = []
        redis_list.append("http://"+ ip01)
        redis_list.append("http://"+ ip02)
        return redis_list