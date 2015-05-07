from scrapy.item import Item, Field

class ECResults(Item):
    constituency_cd = Field()
    state_cd = Field()
    state = Field()
    constituency = Field()
    constituency_id = Field()
    state_id = Field()
    timenow = Field()
    status = Field()
    updated = Field()
    allrows = Field()
    url = Field()
    pass
