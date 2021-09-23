import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import re


def get_page_content(location: str, neighb: str):
    """Retrieves contents from a page from the Invest Komfort site based on the location (city) and neighbourhood"""
    results = requests.get(f"https://www.investkomfort.pl/mieszkania-{location}/{neighb}/")
    if results.status_code == 200:
        return results.content
    else:
        print(f"Could not retrieve the page contents for {location} and {neighb}! HTTP Status code {results.status_code}!")
        return None

    
def retrieve_flat_prices(html_content: str) -> dict:
    """
    @param html_content: A HTML string object with from the invest komfort website
    @return: A dictionary with number of rooms as keys, and a tuple of (min_price, max_price) as values
    """
    if html_content == None:
        return None
    bs_page = BeautifulSoup(html_content,features="html.parser")
    pricing_section = bs_page.find_all(attrs={"class":"pricing"})[0]

    prices = {}

    room_prices_raw = pricing_section.find_all('td',class_='')

    for table_row in room_prices_raw:
        number_of_rooms = table_row.span.string
        room = re.match("\d+",number_of_rooms).group(0)
        price_range = re.match("([\d\s]+) - ([\d\s]+)", table_row.contents[1])
        if price_range:
            min_price = int(price_range.group(1).replace(" ", ""))
            max_price = int(price_range.group(2).replace(" ", ""))
            prices[room] = (min_price,max_price)
        else:
            price_range = re.match("[\d\s]+",table_row.contents[1]).group(0).replace(" ","")
            prices[room] = (price_range, price_range)
    return prices
  

class NeighbourhoodPrices:
    
    def __init__(self, date: datetime.datetime, neighb_name: str, neighb_prices: dict):
        """Object class holding pricing data as of Today for a single neighbourhood from Invest Komfort's website

        Args:
            date (datetime.datetime): Date for when the price data is being loaded
            neighb_name (str): Name of the neighbourhood
            neighb_prices (dict): Dictionary with the pricing data
        """        
        self.date = date
        self.name = neighb_name
        self.neighb_prices = neighb_prices
        
        self.rooms = list(self.neighb_prices.keys())
        self.rooms = []
        self.min_prices = []
        self.max_prices = []
        self.avg_prices = []
        
        for rooms, price_range in self.neighb_prices.items():
            # Add a minimum price (lower bracket)
            self.min_prices.append(int(price_range[0]))
            
            # Add a maximum price (upper bracket)
            self.max_prices.append(int(price_range[1]))
            
            # Add an average price between the two brackets
            self.avg_prices.append((int(price_range[0]) + int(price_range[0])) / 2)
            
            # Add the number of rooms within a flat
            self.rooms.append(int(rooms))

    
    def __str__(self):
        """Returns a string representation of the prices for the neighbourhood"""
        results = f"Pricing for {self.name} at {self.date.strftime('%Y-%m-%d')}:\n----------------------------------------\nRooms\tMin\tMax\tAvg"
        for i in range(0,len(self.rooms)-1):
            results += f"\n{self.rooms[i]}\t{self.min_prices[i]}\t{self.max_prices[i]}\t{self.avg_prices[i]}"
        return results
        
        
    def to_dict(self) -> dict:
        results = {"Neighbourhood": [self.name] * len(self.rooms),
                   "Date": [self.date] * len(self.rooms),
                   "Rooms": self.rooms,
                   "Min Price": self.min_prices,
                   "Max Price": self.max_prices,
                   "Avg Price": self.avg_prices
                  }
        return results
            
    def to_df(self) -> pd.DataFrame:
        """Generates a pandas dataframe with prices for the neighbourhood"""
        return pd.DataFrame(data=self.to_dict())
       


class CityPrices:
    
    def __init__(self, location: str, available_neighbs: list):
        """Object holding price data for date as of today

        Args:
            location (str): Name of the city within the Tricity area (Gdynia/Gdańsk/Sopot)
            available_neighbs (list): List holding strings with names of different neighbourhoods built by InvestKomfort
        """

        self.location = location
        self.neighbs = available_neighbs
        self.data = {}
        
    def get_prices(self):
        """Update the data attribute to hold pricing data"""
        results = {}
        for neighb in self.neighbs:
            if neighb:
                now = datetime.datetime.now().strftime('%Y-%m-%d')
                html_content = get_page_content(self.location, neighb)
                if html_content:
                    prices = retrieve_flat_prices(html_content)
                    results[neighb] = NeighbourhoodPrices(now, neighb,prices)
                else:
                    print(f"Removing {neighb} from the list - could not retrieve information from the website for this neighbourhood")
                    self.neighbs.remove(neighb)
            else:
                self.neighbs.remove(neighb)
        self.data[now] = results
        
    def update_prices(self,date: datetime.datetime, neighb_list: NeighbourhoodPrices):
        self.data[date] = neighb_list
        
    def __iter__(self):
        if self.data:
            for key, neighb_data in self.data.items():
                yield neighb_data
        
    def to_df(self):
        if self.data:
            for date, neighbs_dict in self.data.items():
                results = [pd.DataFrame(data=neighb.to_dict()) for name,neighb in neighbs_dict.items()]
            return pd.concat(results)
        
 
def load_data():
    """Saves a pandas dataframe to a local pickle file"""
    filename = 'price_data.pkl'
    df = pd.read_pickle(filename)
    return df


def save_data(x):
    """Loads a pickle file from the same directory"""
    filename = 'price_data.pkl'
    x.to_pickle(filename)

def main():
    available_neighbourhoods_gdynia = ["portova","silva","nowe-orlowo","nowe-kolibki"]
    gdynia = CityPrices("gdynia",available_neighbourhoods_gdynia)
    gdynia.get_prices()


    sopot = CityPrices('sopot', ['okrzei'])
    sopot.get_prices()

    gdansk = CityPrices('gdansk',['botanica','nadmorski-dwor','gdanska'])
    gdansk.get_prices()

    new_data  = pd.concat([gdynia.to_df(),sopot.to_df(),gdansk.to_df()]).reset_index().drop(columns="index")
    print('Collected the data!')
    print(new_data)
    save_data(new_data)

if __name__ == '__main__':
    main()