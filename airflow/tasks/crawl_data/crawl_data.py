# Import libraries
import requests
from bs4 import BeautifulSoup
import re
import csv

class Crawler:
    def __init__(self) -> None:
        self.base_url = 'https://crinacle.com/rankings/'


    def normalize_header(self, headers: list) -> list:
        new_headers = []
        for header in headers:
            if 'Setup' == header:
                header = 'driver_type'
            # Replace all non_alphanumeric characters in header with a space
            new_header = re.sub(r'[^a-zA-Z0-9]', ' ', header)
            new_headers.append('_'.join(new_header.split()).lower())
        
        return new_headers

    
    def get_data(self, device_type: str):
        url = self.base_url + device_type
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Get all table tag
        table = soup.find_all('table')
        
        # Get headers of table
        thead = table[0].find('thead')
        headers = thead.find_all('th')
        headers = [cell.text for cell in headers]
        headers = self.normalize_header(headers)

        # Get data rows of device type
        tbody = table[1].find('tbody')
        rows = tbody.find_all('tr')
        device_data = []
        device_data.append(headers)     # Add headers to data
        for row in rows:
            row_data = []
            cells = row.find_all('td')
            for cell in cells:
                row_data.append(cell.text)
            device_data.append(row_data)

        return device_data

    def write_to_csv(self, device_type, data: list, data_level):
        with open(f'/opt/airflow/data/{device_type}-{data_level}.csv', mode='w', newline='',encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            # Write the header row to the CSV file
            writer.writerow(data[0])    # The first row is the header
            # Write the remaining data rows to the CSV file
            writer.writerows(data[1:])


    

if __name__ == '__main__':
    crawler = Crawler()
    headphones = crawler.get_data(device_type='headphones')
    iems = crawler.get_data(device_type="iems")

    crawler.write_to_csv(device_type='headphones', data=headphones, data_level= 'bronze')
    crawler.write_to_csv(device_type='iems', data=iems, data_level= 'bronze')