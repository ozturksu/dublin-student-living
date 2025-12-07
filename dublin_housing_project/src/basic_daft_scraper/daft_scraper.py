import math
import requests
from bs4 import BeautifulSoup
import pandas as pd


def start_script():
    url_rent = 'https://www.daft.ie/property-for-rent/ireland'
    url_list_rent = get_url_rent_pagination(url_rent)
    data = []
    for url_list in url_list_rent:
        for url in url_list:
            links = get_url_property(url)
            property_data = get_property_data(links)
            data.extend(property_data)

    get_excel(data)



def make_request(url):
    headers = {'user-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0'}
    response = None
    while response is None:
        try:
            response = requests.get(url, headers=headers)
        except:
            response = None
    return response


#def get_url_sale_pagination():
    #same logic as rent pagination

def get_url_rent_pagination(url):
    html = make_request(url)
    soup = BeautifulSoup(html.text, 'html.parser')
    property_number = int(soup.find('h1', class_='sc-af41020b-0').getText().split(' ')[0].replace(',', ''))
    max_number = math.floor(property_number/20)*20
    all_numbers = [item for item in range(1, max_number+1, 20)]
    urls = [f'https://www.daft.ie/property-for-rent/ireland?page={item}' for item in all_numbers]
    urls = urls[:2]
    return urls

def get_url_property(url):
    html = make_request(url)
    soup = BeautifulSoup(html.text, 'html.parser')
    li_elements = soup.find_all('li', class_='sc-798c155d-3')
    start_url = 'https://www.daft.ie/'

    links = []

    for item in li_elements:
        element = item.find('ul', class_='sc-798c155d-4' )
        if element is not None:
            urls = [start_url + x.find('a').get('href') for x in item.find('ul', class_='sc-798c155d-4').find_all('il', class_='sc-798c155d-3')]
            links.extend(urls)
        else:
            url = start_url + item.find('a').get('href')
            links.append(url)
    return links

def get_property_data(links):
    property_data = []
    for url in links:
        html = make_request(url)
        soup = BeautifulSoup(html.text, 'html.parser')
        try:
            address = soup.find('h1', class_='sc-af41020b-0 lhfJmU').getText()
        except:
            address = None

        try:
            price = int(soup.find('h2', class_='sc-af41020b-0 dMYXLR').getText().split(' ')[0].replace(',', '').replace('€', ''))
        except:
            price = None

        try:
            bed = next(x.getText() for x in soup.find_all('span', class_='sc-cd5b13db-3') if 'Bed' in x.getText())
            bed = int(bed.split(' ')[0])
        except:
            bed = None

        try:    
            bath = next(x.getText() for x in soup.find_all('span', class_='sc-cd5b13db-3') if 'Bath' in x.getText())
            bath = int(bath.split(' ')[0])
        except:
            bath = None

        try:    
            area = next(x.getText() for x in soup.find_all('span', class_='sc-cd5b13db-3') if 'm' in x.getText())
            area = int(area.split(' ')[0])
        except:
            area = None

        try:
            property_features = [x.getText() for x in soup.find_all('li', class_='sc-de135c23-1')] 
        except:
            property_features = None

        if 'for-rent' in url:
            property_type = 'For_rent'
        elif 'for-sale' in url:
            property_type = 'For_sale'

        apartment = { 
            'Address': address,
            'Price': price,
            'Bed': bed,
            'Bath': bath,
            'Area': area,
            'Property_type': property_type,
            'Features': property_features
        }
        property_data.append(apartment)


    return property_data

def get_excel(data):
    df = pd.DataFrame(data)
    excel_file = 'data.xlsx'
    df.to_excel(excel_file, index=False)

if __name__ == '__main__':
    start_script()