import sys
import os
import datetime
import requests
import shutil
import time
import uuid
import base64


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
from urllib.parse import urljoin
from sqlalchemy import Table, Column, Integer, String, MetaData, Text, Boolean, DateTime, ForeignKey, BigInteger, \
    create_engine, insert  # Python SQL toolkit essentials

# Google cloud storage library
from google.cloud import storage

# Import the Secret Manager client library.
from google.cloud import secretmanager

project = 'tenders-284621'

# Instantiates a client for logging
client = google.cloud.logging.Client()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
client.get_default_handler()
client.setup_logging()

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

# Uploads file to Google storage
def upload_blob(source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    bucket_name = "tenders-attachments"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

# Changes every unknown UTF-8 character
def convert_characters(input):
    utf8_letters = ['ą','ę','ć','ź','ż','ó','ł','ń','ś','Ą','Ę','Ć','Ź','Ż','Ó','Ł','Ń','Ś']
    ascii_letters = ['a','e','c','z','z','o','l','n','s','A','E','C','Z','Z','O','L','N','S']
    trans_dict = dict(zip(utf8_letters,ascii_letters))
    out = []
    for l in input:
        out.append(trans_dict[l] if l in trans_dict else l)
    return ''.join(out)

# Link containing all tenders
MAINLINK = "https://www.swz.kghm.pl/rfx/rfx/HomeServlet?MP_module=outErfx&MP_action=supplierStatus&iRfxRound="

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

# Get ID from system arguments
_id = sys.argv[1]
date_published = sys.argv[2]

# Convert date from timestamp to the correct format
date_published = time.strftime(
    "%Y-%m-%d %H:%M", time.localtime(int(date_published)))

# Logging start
logging.info(
    "=================================================================================")
logging.info("Tender scraping started")


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

#DELETE LATER
f = open("server-ca.pem", "r")
logging.info(f.read())


# Postgresql connection strings
DATABASE_HOST = os.environ["DATABASE_HOST"]
DATABASE_CREDENTIALS = secrets_client.access_secret_version(request={"name": "projects/"+project+"/secrets/credentials/versions/1"}).payload.data.decode("utf-8")

# Connect to database
DATABASE_URI = "postgresql://" + DATABASE_CREDENTIALS + "@" + DATABASE_HOST
try:
    engine = create_engine(DATABASE_URI, connect_args=ssl_args)
    connection = engine.connect()
except Exception as e:
    logging.fatal("Can't connect to Postgresql - {}".format(str(e)))
    browser.quit()
    sys.exit(1)

# Retrieve notices/operators table
metadata = MetaData(schema="tenders")
notices_table = Table('notices', metadata, autoload=True, autoload_with=engine)
operators_table = Table('operators', metadata, autoload=True, autoload_with=engine)
items_table = Table('items', metadata, autoload=True, autoload_with=engine)

browser.get(MAINLINK+_id)
time.sleep(4)

logging.info('Went to notice page for #{}'.format(_id))

tree = html.fromstring(browser.page_source)
root_url = browser.current_url

logging.info("Extracting data from notice")


deadline = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Data i godzina zakończenia czasu na składanie ofert")]/../*[2]))')

div = browser.find_element_by_class_name('main')
h = div.find_element_by_tag_name('h2')
tender_name = h.text

tender_number = tree.xpath(
    'normalize-space(string(//span[contains(normalize-space(text()), "Numer postępowania")]/../*[2]))')
supplier_status = tree.xpath(
    'normalize-space(string(//span[contains(normalize-space(text()), "Status oferenta")]/../*[2]))')
stage_number = tree.xpath(
    'normalize-space(string(//span[contains(normalize-space(text()), "Numer etapu")]/../*[2]))')
source_doc = tree.xpath(
    'normalize-space(string(//span[contains(normalize-space(text()), "Dokument źródłowy")]/../*[2]))')
items_count = tree.xpath(
    'normalize-space(string(//span[contains(normalize-space(text()), "Liczba koszyków/ części w")]/../*[2]))')
base_currency = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Waluta postępowania")]/../*[2]))')

elem = wait(browser, 5).until(EC.presence_of_element_located((By.XPATH,
                                                            '//a[contains(@title, "Pokaż dane kontaktowe")]')))
browser.execute_script("arguments[0].click();", elem)
time.sleep(2)

tree = html.fromstring(browser.page_source)

operator_email = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Email")]/../*[2]))')
organisational_unit = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Jednostka organizacyjna")]/../*[2]))')
tender_description = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Opis postępowania")]/../*[2]))')
category = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Grupa asortymentowa")]/../*[2]))')
is_framework_agreement = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Czy umowa ramowa?")]/../*[2]))')
offer_deadline = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Data i godzina")]/../*[2]))')
questions_deadline = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Ostateczny termin")]/../*[2]))')
offer_validity_period = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Wymagany termin")]/../*[2]))')
submitting_offers_type = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Możliwość składania ofert")]/../*[2]))')
language_of_publication = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Język publikacji")]/../*[2]))')
terms_of_participation = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Warunki udziału w postępowaniu")]/../*[2]))')
contract_provosions = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Postanowienia umowy/ zlecenia")]/../*[2]))')

logging.info("Started working on attachments for notice #{}".format(_id))

attachments_name_list = []
for attachment in tree.xpath('//table[contains(@class, "mp_gridTable")]//a[contains(@href, "FileDownload")]'):
    at_name = attachment.xpath('normalize-space(string())')
    at_name = convert_characters(at_name)

    at_link = attachment.xpath('string(@href)')
    at_link = urljoin(root_url, at_link)

    date_type_path = datetime.datetime.strptime(
        date_published, "%Y-%m-%d %H:%M")
    date_path = "{}-{}".format(date_type_path.year, date_type_path.month)

    full_path = os.path.join("temp", date_path, _id)

    if not os.path.exists(full_path):
        os.makedirs(full_path)

    full_path = os.path.join(full_path, at_name)

    # Download to temp folder for later upload to google storage bucket
    try:
        response = requests.get(at_link, stream=True)
        with open(full_path, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        del response
    except Exception as e:
        logging.warning('Failed to download attachment "{}" locally on temp for Notice #{}'.format(at_name, _id))
        pass
    else:
        logging.info('Saved attachment "{}" locally on temp for Notice #{}'.format(at_name, _id))
        attachments_name_list.append(at_name)
        pass

logging.info("Finished downloading attachments locally. Starting upload.")

# Upload files to storage yyyy-mm/id format
attachments_urls_list = []
for attachment_name in attachments_name_list:
    full_path = os.path.join("temp", date_path, _id)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    full_path = os.path.join(full_path, attachment_name)
    attachment_url = date_path+"/"+_id+"/"+attachment_name
    try:
        upload_blob(full_path, attachment_url)
    except Exception as e:
        logging.warning('File "{}" failed to upload.'.format(full_path))
        pass
    else:
        logging.info('File "{}" uploaded to "{}".'.format(full_path, attachment_url))
        attachments_urls_list.append(attachment_url)
        pass

logging.info("Finished working on attachments for notice #{}".format(_id))


currencies_name_list = []
currency_ = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Dostępne waluty")]/../td//table//tr[2]/td[1]))')
currencies_name_list.append(currency_)

logging.info("Extracting operator for Notice #{}".format(_id))

first_name = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Imię")]/../td))')
last_name = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Nazwisko")]/../td))')
address = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Ulica")]/../td))')
city = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Miejscowość")]/../td))')
post_code = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Kod pocztowy")]/../td))')
email = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Email")]/../td))')
phone = tree.xpath(
    'normalize-space(string(//th[contains(normalize-space(text()), "Telefon")]/../td))')

# Converting data to fit column types
is_framework_agreement = False if "nie" in is_framework_agreement else True

try:
    year = deadline.split('(')[0].strip().split(' ')[0].split('-')[0]
    month = deadline.split('(')[0].strip().split(' ')[0].split('-')[1]
    day = deadline.split('(')[0].strip().split(' ')[0].split('-')[2]
    hour = deadline.split('(')[0].strip().split(' ')[1].split(':')[0]
    minute = deadline.split('(')[0].strip().split(' ')[1].split(':')[1]
    deadline = datetime.datetime(int(year), int(
        month), int(day), int(hour), int(minute))
except:
    deadline = None

try:
    year = offer_deadline.split(' ')[0].split('-')[0]
    month = offer_deadline.split(' ')[0].split('-')[1]
    day = offer_deadline.split(' ')[0].split('-')[2]
    hour = offer_deadline.split(' ')[1].split(':')[0]
    minute = offer_deadline.split(' ')[1].split(':')[1]
    offer_deadline = datetime.datetime(
        int(year), int(month), int(day), int(hour), int(minute))
except:
    offer_deadline = None

try:
    year = questions_deadline.split(' ')[0].split('-')[0]
    month = questions_deadline.split(' ')[0].split('-')[1]
    day = questions_deadline.split(' ')[0].split('-')[2]
    hour = questions_deadline.split(' ')[1].split(':')[0]
    minute = questions_deadline.split(' ')[1].split(':')[1]
    questions_deadline = datetime.datetime(
        int(year), int(month), int(day), int(hour), int(minute))
except:
    questions_deadline = None

query = insert(operators_table).values(
    first_name=first_name,
    last_name=last_name,
    address=address,
    city=city,
    postcode=post_code,
    email=email,
    phone=phone
)

try:
    result_proxy = connection.execute(query)
except:
    pass

logging.info('Operator saved/already been saved: {}'.format(email))


query = insert(notices_table).values(
    id=_id,
    date_published=date_published,
    deadline=deadline,
    tender_name=tender_name,
    tender_number=tender_number,

    supplier_status=supplier_status,
    stage_number=stage_number,
    source_doc=source_doc,
    items_count=int(items_count),
    base_currency=base_currency,
    operator_email=operator_email,
    organisational_unit=organisational_unit,
    tender_description=tender_description,
    category=category,
    is_framework_agreement=is_framework_agreement,

    offer_deadline=offer_deadline,
    questions_deadline=questions_deadline,
    offers_validity_period=int(offer_validity_period),
    submitting_offers_type=submitting_offers_type,
    language_of_publication=language_of_publication,
    terms_of_participation=terms_of_participation,
    contract_provisions=contract_provosions,
    attachments=attachments_name_list,
    attachments_urls=attachments_urls_list,
    currencies=currencies_name_list
)

try:
    result_proxy = connection.execute(query)
except Exception as e:
    logging.error("An error occured - {}".format(e))
    browser.quit()
    sys.exit(1)
    pass

# Extracting items
elem = wait(browser, 5).until(EC.presence_of_element_located((By.XPATH,
                                                                '//a[contains(normalize-space(text()), "Oferta")]')))
browser.execute_script("arguments[0].click();", elem)
time.sleep(4)
elem = wait(browser, 5).until(EC.presence_of_element_located((By.XPATH,
                                                                '//select[contains(@name, "GD_pagesize")]')))
elem.click()
elem = wait(browser, 5).until(EC.presence_of_element_located((By.XPATH,
                                                                '//select[contains(@name, "GD_pagesize")]/option[@value="100"]')))
elem.click()
time.sleep(4)
logging.info('Went to Oferta for notice #{}'.format(_id))
tree = html.fromstring(browser.page_source)
item_id_list = []

for item in tree.xpath('//table[contains(@class, "mp_gridTable")]//tr[contains(@class, "dataRow")]'):
    item_id_list.append(
        item.xpath('string(@id)')
    )

for item_id in item_id_list:
    item_link = 'https://www.swz.kghm.pl/rfx/servlet/HomeServlet?MP_module=outErfx&MP_action=outerPositionDetails&iRequestPosition=656276&iRfxRound=458987'
    item_link = item_link.replace(
        "iRequestPosition=656276&", "iRequestPosition=" + str(item_id).strip() + "&")
    item_link = item_link.replace(
        "iRfxRound=458987", "iRfxRound=" + _id)
    browser.get(item_link)
    time.sleep(4)

    logging.info(
        'Extracting oferta #{} for notice #{}'.format(item_id, _id))

    tree = html.fromstring(browser.page_source)
    name = tree.xpath(
        'normalize-space(string(//th[contains(normalize-space(text()), "Nazwa")]/../*[2]))')
    quantity = tree.xpath(
        'normalize-space(string(//th[contains(normalize-space(text()), "Ilość")]/../*[2]))')
    details = tree.xpath(
        'normalize-space(string(//th[contains(normalize-space(text()), "Opis")]/../*[2]))')
    units = tree.xpath(
        'normalize-space(string(//th[contains(normalize-space(text()), "Jednostka miary")]/../*[2]))')
    supply_date = tree.xpath(
        'normalize-space(string(//th[contains(normalize-space(text()), "Termin wykonania")]/../*[2]))')
    bid_bond_amount_percent = tree.xpath(
        'normalize-space(string(//th[contains(normalize-space(text()), "Wysokość należytego zabezpieczenia wykonania umowy w %")]/../*[2]))')
    
    # Bid bond amount percent
    if bid_bond_amount_percent == '':
        bid_bond_amount_percent = 0
    else:
        bid_bond_amount_percent = int(float(bid_bond_amount_percent.replace(',','.')))

    query = insert(items_table).values(
        id=str(uuid.uuid1()),
        notice_id=_id,
        name=name,
        quantity=quantity,
        description=details,
        units=units,
        supply_date=supply_date,
        bid_bond_amount_percent=bid_bond_amount_percent
    )
    
    try:
        result_proxy = connection.execute(query)
    except Exception as e:
        logging.error("An error occured - {}".format(e))
        browser.quit()
        sys.exit(1)
        pass

    logging.info(
        'Oferta  #{} for notice #{} saved.'.format(item_id, _id))

logging.info('Notice #{} successfully saved.'.format(_id))
browser.quit()
