from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import json
from urllib.parse import urlparse
import psycopg2
import os

# Load the configuration from config.json
with open('production_config.json', 'r') as file:
    config = json.load(file)

def filter_exact_class_length(css_class, class_name, length):
    return class_name in css_class and len(css_class) == length


print("running jobs scraper")
def scrape_company_careers_selenium(company_url, company_jobs_class, company_jobs_title_class, company_jobs_location_class):
    print("scrape_company")
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(company_url)
    print("chromedriver")
    # Wait for the careers listings container to be present
    WebDriverWait(driver, 180).until(
        EC.presence_of_element_located((By.CLASS_NAME, company_jobs_class))
    )
    print("afterwebdriverwait")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    jobs = soup.find_all(class_=company_jobs_class)
    
    company_job_postings = []
    base_url = urlparse(company_url).scheme + "://" + urlparse(company_url).netloc
    
    for job in jobs:
        print(f"job: {job}")
        # set allows to remove duplicates
        url_elements = []

        title_elements = job.find_all(class_=company_jobs_title_class)
        location_elements = job.find_all(class_=company_jobs_location_class)


        all_url_elements = job.find_all('a')
        # url_elements.update([url_element['href'] for url_element in all_url_elements if url_element.get('href')])

        
        for url_element in all_url_elements:
            href = url_element.get('href')
            if href not in url_elements:
                url_elements.append(href)

        print(f"TElem ({len(title_elements)}): {title_elements}")
        print(f"LElem ({len(location_elements)}): {location_elements}")
        print(f"UElem ({len(url_elements)}): {url_elements}")

        for title_element, location_element, url_element in zip(title_elements, location_elements, url_elements):
            if title_element:
                title = title_element.get_text(strip=True)
            else:
                title = "Title not found"
            
            if location_element:
                location = location_element.get_text(strip=True)
            else:
                location = "Location not found"

            if url_element:
                if url_element.startswith('http'):
                    link = url_element
                else:
                    link = base_url + url_element
            else:
                link = "Link not found"
 
            titles_to_check = ["Product", "Operations","Founder"]
            locations_to_check = ["Tulsa", "Remote", "AMER"]
            if any(string.lower() in title.lower() for string in titles_to_check):
                if any(string.lower() in location.lower() for string in locations_to_check):
                    company_job_postings.append({
                        'title': title,
                        'link': link,
                        'location': location
                    })

    print(f"len: {len(company_job_postings)}")
    driver.quit()
    return company_job_postings


# Modify scrape_all_companies to use scrape_company_careers_selenium
def scrape_all_companies_selenium():
    all_job_postings = []
    for company in config:
        url = company['url']
        jobs_class = company['jobs_class']
        title_class = company['title_class']
        location_class = company['location_class']

        all_job_postings.extend(
                scrape_company_careers_selenium(url, jobs_class, title_class, location_class)
            )
        
    return all_job_postings

def insert_scraped_data_into_database(all_job_postings):
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    db_host = config['db_host']
    db_port = config['db_port']
    db_name = config['db_name']
    db_user = config['db_user']
    db_password = config['db_password']

    # Connect to the database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()

    # Assuming 'all_job_postings' is the array of job data
    for job in all_job_postings:
        title = job['title']
        link = job['link']
        location = job['location']

        # Insert data into the 'jobs' table
        sql = "INSERT INTO jobs (title, link, location) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
        values = (title, link, location)
        cursor.execute(sql, values)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': 'Data inserted into PostgreSQL'
    }
    
# Execute the scraper function in lambda
def lambda_handler():
    scraped_data = scrape_all_companies_selenium()
    insert_scraped_data_into_database(scraped_data)
    print(scraped_data)

lambda_handler()