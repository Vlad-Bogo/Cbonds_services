from datetime import datetime, date
from CbondsExRates import CbondsApi

quotes = CbondsApi('*****', '*****') # Initiate object with login and password passed
quotes.set_api_url('https://ws2.cbonds.info/services/json/get_index_quotes_intraday/') # Provide API data link
quotes.set_filters([{"field":"date","operator":"eq","value": str(date.today())},
                    {"field":"time","operator":"gt","value":'14:39:00'},
                    {"field":"time","operator":"lt","value":'14:41:00'},
                    {"field":"index_category_id","operator":"eq","value":'6'},
                    {"field":"index_group_id","operator":"eq","value":'4'}])
quotes.set_quantity({"limit": 1000,"offset": 0})
quotes.set_sorting([{"field":"id","order":"asc"}])
quotes.set_fields([{"field": "index_id"}, {"field": "emitent_id"}, {"field": "date"}, {"field": "time"},
                   {'field': 'mid'}, {'field': 'updated_at'}])
quotes.execute()  # Call main method for request processing
quotes.save(f'ex_rates_{datetime.now().strftime("%Y%m%d")}.xlsx')  # Optional call for file generating method
