import sys
import time
import datetime
import os

# Google Tasks API
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import json
import base64

# Import the Secret Manager client library.
from google.cloud import secretmanager

# Selenium essentials
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Imports the Cloud Logging client library
import google.cloud.logging
# Imports Python standard library logging
import logging

import chromedriver_binary  # Adds chromedriver binary to path
from lxml import html
from sqlalchemy import Table, Column, Integer, String, MetaData, Text, Boolean, DateTime, ForeignKey, BigInteger, \
    create_engine, insert  # Python SQL toolkit essentials

# Instantiates a client for logging
client = google.cloud.logging.Client()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
client.get_default_handler()
client.setup_logging()

# Create a client for tasks API.
client = tasks_v2.CloudTasksClient()

# Tasks API Configuration
project = 'tenders-284621'
queue = 'scraping-queue'
location = 'europe-west1'
BASEURL = os.environ["BASEURL"]
payload = None
task_name = None


# client-cert, client-key, server-ca
def create_pem(pem_type, pem_content):
    with open(pem_type+".pem", "w+", encoding='utf8', newline='\n') as fh:
        fh.write(pem_content[:pem_content.find('-----', 1)+5]+'\n')
        
        value = pem_content[pem_content.find('-----', 1)+5:pem_content.rfind('-----END')]
        for i in range(0, len(value) // 64):
            fh.write(value[64*i:64*(i+1)]+'\n')
        if (len(value) % 64 != 0):
            fh.write(value[64*(len(value)//64)::]+'\n')     
        
        fh.write(pem_content[pem_content.rfind('-----END'):])

        os.chmod(pem_type+".pem", 0o600)
        fh.close()


# Create the Secret Manager client.
secrets_client = secretmanager.SecretManagerServiceClient()

# Access the secret version.
client_cert = secrets_client.access_secret_version(request={"name": "projects/"+project+"/secrets/db-client-cert/versions/1"}).payload.data.decode("utf-8")
client_key = secrets_client.access_secret_version(request={"name": "projects/"+project+"/secrets/db-client-key/versions/1"}).payload.data.decode("utf-8")
server_ca = secrets_client.access_secret_version(request={"name": "projects/"+project+"/secrets/db-server-ca/versions/1"}).payload.data.decode("utf-8")


create_pem('client-cert', base64.b64decode(client_cert).decode("utf-8"))
create_pem('client-key', base64.b64decode(client_key).decode("utf-8"))
create_pem('server-ca', base64.b64decode(server_ca).decode("utf-8"))



# Google Cloud SSL Configuration
ssl_args = {'sslrootcert':'server-ca.pem',
            'sslcert':'client-cert.pem',
            'sslkey':'client-key.pem'}

# Construct the fully qualified queue name.
parent = client.queue_path(project, location, queue)

# Link containing all tenders
MAINLINK = "https://www.swz.kghm.pl/servlet/HomeServlet?MP_module=main&MP_action=noticeList&demandType=nonpublic"
# Postgresql connection strings
DATABASE_HOST = os.environ["DATABASE_HOST"]
DATABASE_CREDENTIALS = secrets_client.access_secret_version(request={"name": "projects/"+project+"/secrets/credentials/versions/1"}).payload.data.decode("utf-8")

time_in_between = 15

# Set log file and log level (INFO/DEBUG)
logging.info("=================================================================================")
logging.info("Scraping all tenders started")

# The following options are required to make headless Chrome work in a Docker container
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1024,768")
chrome_options.add_argument("--no-sandbox")

# Initialize a new browser
try:
    browser = webdriver.Chrome(
        options=chrome_options)
except Exception as e:
    logging.fatal("Browser didn't start - {}".format(str(e)))
    sys.exit(1)

logging.info("Browser started")

# Connect to database
DATABASE_URI = "postgresql://" + DATABASE_CREDENTIALS + "@" + DATABASE_HOST
try:
    engine = create_engine(DATABASE_URI, connect_args=ssl_args)
    connection = engine.connect()
except Exception as e:
    logging.fatal("Can't connect to Postgresql - {}".format(str(e)))
    browser.quit()
    sys.exit(1)

# Retrieve notices table
metadata = MetaData(schema="tenders")
notices_table = Table('notices', metadata, autoload=True, autoload_with=engine)

# Access link and scrape notices general information
browser.get(MAINLINK)
time.sleep(2)

logging.info("Site opened")
elem = wait(browser, 5).until(EC.presence_of_element_located((By.XPATH,
                                                              '//select[contains(@name, "GD_pagesize")]')))
elem.click()
elem = wait(browser, 5).until(EC.presence_of_element_located((By.XPATH,
                                                              '//select[contains(@name, "GD_pagesize")]/option[@value="100"]')))
elem.click()
time.sleep(2)
tree = html.fromstring(browser.page_source)
count_check = tree.xpath(
    'string(count(//table[contains(@class, "bodybox")]//tr[@onmouseover]//img[@src="/pic/mp/details.gif"]/../@href))')

logging.info('Number of tenders: {}'.format(count_check))
# Will contain all new notices
notices = []
in_seconds = time_in_between
for attr in tree.xpath('//table[contains(@class, "bodybox")]//tr[@onmouseover]'):
    url = attr.xpath('string(.//img[@src="/pic/mp/details.gif"]/../@href)')
    if url:
        id_ = url.split("iRfxRound=")[-1]

        date_published_string = attr.xpath(
            'normalize-space(string(.//td[4]))')

        query = notices_table.select().where(notices_table.c.id == id_)
        result = connection.execute(query)

        length = 0
        for row in result:
            length += 1

        date_published = date_published_string[:16]
        date_published = time.mktime(datetime.datetime.strptime(date_published, "%Y-%m-%d %H:%M").timetuple())

        # Notice never scraped
        if (length == 0):            
            # Construct the request body.
            url = BASEURL +str(id_)+'/'+str(date_published)[:-2] 
            task = {
                'http_request': {  # Specify the type of request.
                    'http_method': 'GET',
                    'url': url  # The full url path that the task will be sent to.
                }
            }

            # Create task to run in cloud
            if payload is not None:
                if isinstance(payload, dict):
                    # Convert dict to JSON string
                    payload = json.dumps(payload)
                    # specify http content-type to application/json
                    task['http_request']['headers'] = {'Content-type': 'application/json'}

                # The API expects a payload of type bytes.
                converted_payload = payload.encode()

                # Add the payload to the request.
                task['http_request']['body'] = converted_payload

            if in_seconds is not None:
                # Convert "seconds from now" into an rfc3339 datetime string.
                d = datetime.datetime.utcnow() + datetime.timedelta(seconds=in_seconds)

                # Create Timestamp protobuf.
                timestamp = timestamp_pb2.Timestamp()
                timestamp.FromDatetime(d)

                # Add the timestamp to the tasks.
                task['schedule_time'] = timestamp

            if task_name is not None:
                # Add the name to tasks.
                task['name'] = task_name

            # Use the client to build and send the task.
            response = client.create_task(parent, task)
            logging.info('Created task {} for tender #{} at {}'.format(response.name, id_, url))

            # Increment in_seconds to seperate workload
            in_seconds += time_in_between

            # py scrape_id.py notice date_published
            notices.append({
                "tender_id": id_,
                "date_published": str(date_published)[:-2]
            })

browser.quit()
for notice in notices:
    logging.info(notice)
logging.info("Script completed with {} new notices.".format(len(notices)))
logging.info("=================================================================================")
