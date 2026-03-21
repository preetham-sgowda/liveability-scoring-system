# Automatically created by Scrapy
BOT_NAME = "bbmp_sahaaya"

SPIDER_MODULES = ["bbmp_sahaaya.spiders"]
NEWSPIDER_MODULE = "bbmp_sahaaya.spiders"

# Crawl responsibly
ROBOTSTXT_OBEY = True
USER_AGENT = (
    "LSS-DataBot/1.0 (+https://github.com/lss-project)"
)

# Concurrent requests
CONCURRENT_REQUESTS = 4
DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 2

# Disable cookies
COOKIES_ENABLED = False

# Enable retry middleware
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# Pipelines
ITEM_PIPELINES = {
    "bbmp_sahaaya.pipelines.PostgresPipeline": 300,
}

# Logging
LOG_LEVEL = "INFO"

# AutoThrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
