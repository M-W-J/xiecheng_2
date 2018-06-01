# -*- coding: utf-8 -*-

# Scrapy settings for xiecheng_scrapy project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'xiecheng_scrapy'
# LOG_LEVEL = 'INFO'

SPIDER_MODULES = ['xiecheng_scrapy.spiders']
NEWSPIDER_MODULE = 'xiecheng_scrapy.spiders'

# 增量爬取配置
# SPIDER_MIDDLEWARES = { 
# "scrapy_deltafetch.DeltaFetch": 100 
# } 
# DELTAFETCH_ENABLED = True


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
# 打印日志
LOG_FILE = 'xiecheng.log'
# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# 使用的是scrapy_redis的去重类
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# 调度器使用是scrapy_redis的调度器
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
# 爬取的过程中是否允许暂停
SCHEDULER_PERSIST = True

# 配置存储的redis服务器
REDIS_HOST = '10.0.0.97'
REDIS_PORT = 6379

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 2
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
	'Accept-Language': 'en',
}

# nmongodb配置信息
MONGO_HOST = "10.0.0.88"  
MONGO_PORT = 27017  
MONGO_DB = "ctrip03"  
SCENIC_COMMENTS_COLL = "comment"  
SCENIC_INFO_COLL = "scenic_info"  
SCENIC_TICKES_COLL = "scenic_tickes"  
SCENIC_PAGE_COLL = "scenic_page"  
COMMENT_PAGE_COLL = "comment_page"  

SYS_COUNT = 1

#mysql配置信息
# DB_HOST = '10.0.0.237'
# DB_PORT = 3306
# DB_USER = 'root'
# DB_PWD = '1234azsy'
# DB_NAME = 'test'
# DB_CHARSET = 'utf8'

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'xiecheng_scrapy.middlewares.XiechengScrapySpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   'xiecheng_scrapy.middlewares.Proxy': 543,

}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'xiecheng_scrapy.pipelines.XiechengScrapyPipeline': 300,
   # 'xiecheng_scrapy.pipelines.JsonPipeline': 299,
   # 'scrapy_redis.pipelines.RedisPipeline': 400,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
