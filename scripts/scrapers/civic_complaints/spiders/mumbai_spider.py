import scrapy
from civic_complaints.items import CivicComplaintItem
from datetime import datetime

class MumbaiSpider(scrapy.Spider):
    name = "mumbai"
    city = "Mumbai"
    allowed_domains = ["portal.mcgm.gov.in"]
    start_urls = ["https://portal.mcgm.gov.in/irj/portal/anonymous/qlcomplaint"]

    def parse(self, response):
        for row in response.css(".complaint-row"):
            item = CivicComplaintItem()
            item['city'] = self.city
            item['complaint_id'] = row.css(".id::text").get()
            item['category'] = row.css(".cat::text").get()
            item['ward_name'] = row.css(".ward::text").get()
            item['status'] = row.css(".status::text").get()
            item['date_filed'] = datetime.now().strftime("%Y-%m-%d")
            yield item
