BOT_NAME = "civic_complaints"

SPIDER_MODULES = ["civic_complaints.spiders"]
NEWSPIDER_MODULE = "civic_complaints.spiders"

ROBOTSTXT_OBEY = False

ITEM_PIPELINES = {
    "civic_complaints.pipelines.PostgresPipeline": 300,
}

DOWNLOAD_DELAY = 1.0
CONCURRENT_REQUESTS = 16

# Session/Cookie handling
COOKIES_ENABLED = True

# Logging
LOG_LEVEL = "INFO"
