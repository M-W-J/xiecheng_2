from scrapy.utils.project import get_project_settings
import redis
import pymongo


# 信息新增处理

settings = get_project_settings()
client = pymongo.MongoClient(host=settings['MONGO_HOST'], port=settings['MONGO_PORT'])      
db = client[settings['MONGO_DB']]  
scenic_comments_coll = db[settings['SCENIC_COMMENTS_COLL']] 
scenic_info_coll = db[settings['SCENIC_INFO_COLL']]  
scenic_tickes_coll = db[settings['SCENIC_TICKES_COLL']] 
scenic_page_coll = db[settings['SCENIC_PAGE_COLL']]
comment_page_coll = db[settings['COMMENT_PAGE_COLL']]
r = redis.Redis(host="10.0.0.97",port=6379,db=1)
mark = int(r.get("mark15").decode('utf-8'))
settings = get_project_settings()


if mark > settings['SYS_COUNT']:
    old_comments_scenic_id = scenic_comments_coll.distinct('scenic_id')
    old_tickets_scenic_id = scenic_tickes_coll.distinct('scenic_id')
    
# 评论新增
def comment_add():
    print('comment_add被调用')
    scenic_com_dict = {}   
    for scenic_id in old_comments_scenic_id:
        scenic_com_dict[scenic_id] = [i['comment_id'] for i in scenic_comments_coll.find({'scenic_id':scenic_id}).sort('comment_id',-1).limit(5)]
    return scenic_com_dict

#  评论原网页新增
def com_page_add():
    print('com_page_add被调用')
    com_page_dict = {}
    for scenic_id in old_comments_scenic_id:
        com_page_dict[scenic_id] = [i['comment_page_id'] for i in comment_page_coll.find({'scenic_id':scenic_id}).sort('comment_page_id',-1).limit(2)] 
    return com_page_dict

# 景点新增
def scenic_add():
    print('scenic_add被调用')
    scenic_info_list = [i['scenic_id'] for i in scenic_info_coll.find({})]
    return scenic_info_list

# 门票新增
def scenic_pickets_add():
    print('scenic_pickets_add被调用')
    scenic_pisckes_dict = {}
    # scenic_id_list = scenic_tickes_coll.distinct('scenic_id')
    for scenic_id in old_tickets_scenic_id:
        scenic_pisckes_dict[scenic_id] = [i['ticket_id'] for i in scenic_tickes_coll.find({'scenic_id':scenic_id})]
    return scenic_pisckes_dict
    