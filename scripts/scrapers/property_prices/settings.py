BOT_NAME = "property_prices"
SPIDER_MODULES = ["property_prices.spiders"]
NEWSPIDER_MODULE = "property_prices.spiders"
ROBOTSTXT_OBEY = False
ITEM_PIPELINES = {"property_prices.pipelines.PostgresPipeline": 300}
DOWNLOAD_DELAY = 2.0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
