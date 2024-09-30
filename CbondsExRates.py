import requests
import re
import pandas as pd
import time
from datetime import datetime, date

class CbondsApi:
    """ 
    Interaction with the Cbonds web-service and downloading data
    """ 

    # The list of credentials for API call
    __credentials = {
        "auth":{"login":"","password":""},
        "filters":[],
        "quantity":[],
        "sorting":[],
        "fields": []
    }

    # The API url to send request for
    __api_url = ''
    
    # The update time
    maxdate = []
    
    # Output data from from API call
    __result = pd.DataFrame()

    def __init__(self, login, password):
        self.__credentials['auth']['login'] = login
        self.__credentials['auth']['password'] = password

    def set_api_url(self, url):
        self.__api_url = url
        return self

    def set_filters(self, filters):
        self.__credentials['filters'] = filters
        return self

    def set_quantity(self, quantity):
        self.__credentials['quantity'] = quantity
        return self

    def set_sorting(self, sorting):
        self.__credentials['sorting'] = sorting
        return self
    
    def set_fields(self, fields):
        self.__credentials['fields'] = fields
        return self

    def execute(self):
        """ 
        Main method for API request.
        """ 
        response = requests.post(self.__api_url, json=self.__credentials).json()
        if 'total' in response:
            total = response['total']
            if total != 0:
                self.__result = pd.json_normalize(response['items'])
                self.__credentials['quantity']['offset'] += self.__credentials['quantity']['limit']
                print(f"выгружено {self.__credentials['quantity']['offset']} записей...")
                for offset in range(self.__credentials['quantity']['offset'], total, self.__credentials['quantity']['limit']):
                    time.sleep(2.1)
                    response = requests.post(self.__api_url, json=self.__credentials).json()
                    tmp = pd.json_normalize(response['items'])
                    self.__result = pd.concat([self.__result,tmp], ignore_index=True)
                    self.__credentials['quantity']['offset'] += self.__credentials['quantity']['limit']
                    print(f"выгружено {self.__credentials['quantity']['offset']} записей...")
                    
                self.maxdate = self.__result.agg({'updated_at': ['max']})
            else:
                print('За выбранный период данные отсутствуют')
        else:
            print('Проблема соединения с сервером')
            
        return self.__result

    def save(self, file_path):
        if not self.__result.empty:  
            self.__result['mid'] = pd.to_numeric(self.__result['mid'])
            self.__result.set_index('Type_rus', inplace = True)
            if file_path[-4:] == 'xlsx': self.__result.to_excel(file_path)
            elif file_path[-4:] == '.csv': self.__result.to_csv(file_path, sep = ';', decimal=',')
            print('Данные сохранены.')
        else:
            print('No data')

if __name__ == '__main__':
    web_service = CbondsApi('*****', '*****') # Initiate object with login and password passed
    web_service.set_api_url('https://ws2.cbonds.info/services/json/get_index_quotes_intraday/') # Provide API data link
    web_service.set_filters([{"field":"date","operator":"eq","value": str(date.today())},
                   {"field":"time","operator":"gt","value":'15:28:00'},
                   {"field":"time","operator":"lt","value":'15:30:00'},
                   {"field":"index_category_id","operator":"eq","value":'6'},
                   {"field":"index_group_id","operator":"eq","value":'4'}])
    web_service.set_quantity({"limit": 1000,"offset": 0})
    web_service.set_sorting([{"field":"id","order":"asc"}])
    web_service.set_fields([{"field": "index_id"}, {"field": "emitent_id"}, {"field": "date"}, {"field": "time"},
                   {'field': 'mid'}, {'field': 'updated_at'}])
    
    quotes = web_service.execute()  # Call main method for request processing
    if not quotes.empty:
        FX_types = pd.read_csv('C:\\Users\\bogoslovskiyvd\\Documents\\Курсы валют\\Cbonds\\FX_types.csv', sep=';') # The catalogue of quote types
        FX_info = pd.read_excel('Справочник редких валют.xlsx').rename(columns = {'Букв. код':'Type_rus'})
        quotes.index_id = quotes.index_id.astype(int)
        select_col = quotes.columns.isin(['id', 'updated_at'])
        ex_rates = pd.merge(quotes.loc[:, ~select_col], FX_types[['index_id', 'Type_rus']], on='index_id')
        cur = ex_rates.Type_rus.tolist()
        ind=[]
        for i, x in enumerate(cur):
            if i == cur.index(x):
                ind.append(True)
            else:
                ind.append(False)   
        ex_rates = ex_rates[ind]
            
        ex_rates = ex_rates.loc[ind, ['Type_rus', 'mid']]
        ex_rates.Type_rus = [re.sub('USD/|/USD', '', cur, count=0) for cur in ex_rates.Type_rus]
        ex_rates = pd.merge(ex_rates, FX_info, on='Type_rus')
        ex_rates = ex_rates.rename(columns = {'Type_rus':'Букв. код', 'mid':'Курс'})
        ex_rates = ex_rates[['Цифр. код', 'Букв. код', 'Валюта', 'Курс']]
        
        #web_service.save(f'ex_rates_{datetime.now().strftime("%Y%m%d")}.xlsx')  # Optional call for file generating method
        ex_rates['Курс'] = pd.to_numeric(ex_rates['Курс'])
        ex_rates.to_excel(f'ex_rates_{datetime.now().strftime("%Y%m%d")}.xlsx', index=False)
        print(r"Данные сохранены")
