import scrapy
from civic_complaints.items import CivicComplaintItem
from datetime import datetime

class DelhiSpider(scrapy.Spider):
    name = "delhi"
    city = "Delhi"
    allowed_domains = ["mcdonline.nic.in"]
    start_urls = ["https://mcdonline.nic.in/portal/complaint"]

    def parse(self, response):
        for row in response.css(".complaint-item"):
            item = CivicComplaintItem()
            item['city'] = self.city
            item['complaint_id'] = row.css(".ref::text").get()
            item['category'] = row.css(".type::text").get()
            item['ward_name'] = row.css(".zone::text").get()
            item['status'] = row.css(".status::text").get()
            item['date_filed'] = datetime.now().strftime("%Y-%m-%d")
            yield item
