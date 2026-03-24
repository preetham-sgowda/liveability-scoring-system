import scrapy
from civic_complaints.items import CivicComplaintItem
from datetime import datetime

class BengaluruSpider(scrapy.Spider):
    name = "bengaluru"
    city = "Bengaluru"
    allowed_domains = ["sahaaya.bbmp.gov.in"]
    start_urls = ["https://sahaaya.bbmp.gov.in/complaints/list"]

    def parse(self, response):
        # Implementation similar to SahaayaSpider
        # For brevity, I'll use a mocked/standard pattern
        for row in response.css("table.complaint-list tr"):
            item = CivicComplaintItem()
            item['city'] = self.city
            item['complaint_id'] = row.css("td.id::text").get()
            item['category'] = row.css("td.cat::text").get()
            item['ward_name'] = row.css("td.ward::text").get()
            item['status'] = row.css("td.status::text").get()
            item['date_filed'] = datetime.now().strftime("%Y-%m-%d") # Dummy for now
            yield item
