from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.common.by import By
from clickhouse_driver import Client
import pendulum
import requests
import os, csv
from datetime import datetime


URL='https://www.nalog.gov.ru/rn77/program/5961290/'

@dag(
    dag_id='customs_enriched',
    start_date=pendulum.datetime(2023, 1 , 1),
    schedule=None,
    tags=['slurm_de'],
    catchup=False,
)


def Customs_Enrich():
    @task 
    def ping_site():
        response = requests.get(URL, headers={'User-Agent': 'Chrome/50.0.2661.102'})
        response.raise_for_status()
    
    
    @task.branch
    def check_relevant_date(ti=None):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')

        driver = webdriver.Chrome(options=options)
        driver.get(URL)
        relevant_date_element = driver.find_element(
            By.XPATH,
            ('//span[contains(text(), "Р”Р°С‚Р° Р°РєС‚СѓР°Р»СЊРЅРѕСЃС‚Рё")]')
            );
        relevant_date_from_site = datetime.strptime(
            relevant_date_element.text.split(' ')[2],
            '%d.%m.%Y'
            ).date()
        relevant_date = datetime.strptime(
            Variable.get('relevant_date'),
            '%d.%m.%Y'
            ).date()
        if not relevant_date:
            relevant_date = datetime.fromtimestamp(1)
        link_element = driver.find_elements(
            By.XPATH,
            ('//a[contains(text(), "TNVED.ZIP")]')
            )
        link = link_element[0].get_attribute('href')
        if relevant_date_from_site > relevant_date:
            ti.xcom_push(
                key='new_relevant_date',
                value=relevant_date_from_site.strftime('%d.%m.%Y')
                )
            ti.xcom_push(key='link_to_archive', value=link)
            return 'download_categories'
    
    
    @task
    def download_categories(ti=None):
        response = requests.get(
            ti.xcom_pull(
                task_ids='check_relevant_date',
                key='link_to_archive'),
            headers={'User-Agent': 'Chrome/50.0.2661.102'},
            )
        response.raise_for_status
        os.makedirs('/tmp/slurm_de_8', exist_ok=True)
        with open('/tmp/slurm_de_8/tnved.zip', 'wb') as file:
            file.write(response.content)

    unzip_archive = BashOperator(
        task_id='unzip_archive',
        bash_command='unzip -o /tmp/slurm_de_8/tnved.zip -d /tmp/slurm_de_8/',
    )
    
    repair_encoding = BashOperator(
        task_id='repair_encoding',
        bash_command='iconv -f CP866 "/tmp/slurm_de_8/TNVED3.TXT" -t UTF-8 > \
            /tmp/slurm_de_8/TNVED3_UTF.TXT',
    )

    
    @task
    def map_categories():
        categories={}
        with open(
            '/tmp/slurm_de_8/TNVED3_UTF.TXT',
            'r',
            encoding='UTF-8'
        ) as tnved:
            reader = csv.reader(tnved, delimiter='|')
            next(reader)
            for row in reader:
                if row[4] == '':
                    category_id = row[0]+row[1]
                    categories[category_id]=row[2]
            return categories
    
    
    @task
    def map_customs_log():
        log_categories_count = {}
        with open(
            '/tmp/customs_log.csv',
            'r',
            encoding='UTF-8'
        ) as log:
            reader = csv.reader(log, delimiter='\t')
            next(reader)
            for row in reader:
                log_categories_count[row[3]] = \
                    log_categories_count.get(row[3], 0) + 1
            return log_categories_count

    
    @task
    def reduce(ti=None):
        categories=ti.xcom_pull(task_ids='map_categories', key='return_value')
        log_categories_count=ti.xcom_pull(
            task_ids='map_customs_log',
                key='return_value'
                )
        with open('/tmp/slurm_de_8/result.csv', 'w') as file:
            file.write('code\tcount\tcategory\n')
            for row in log_categories_count:
                try:
                    file.write(f'''"{row}"\t{log_categories_count[row]}\t\
                               "{categories[row[:4]]}"\n''')
                except KeyError:
                    file.write(f'"{row}"\t{log_categories_count[row]}\t"РџР РћР§Р•Р•"\n')


    @task
    def fill_db():
        def row_reader():
            with open('/tmp/slurm_de_8/result.csv', 'r') as file:
                for line in csv.reader(file, delimiter='\t'):
                    yield line

        client = Client('10.128.0.3', user='slurm', password='slurm')
        client.execute("""
                CREATE TABLE slurm_de.customs_log
                (
                    `code` String,
                    `count` String,
                    `category` String
                )
                ENGINE = MergeTree
                ORDER BY code
                SETTINGS index_granularity = 8192;""")
        client.execute("INSERT INTO slurm_de.customs_log VALUES", (line for line in row_reader()))


    @task
    def set_new_relevant_date(ti=None):
        new_relevant_date = ti.xcom_pull(
                task_ids='check_relevant_date',
                key='new_relevant_date',
                )
        Variable.set('relevant_date', new_relevant_date)

    ping_site() >> check_relevant_date() >> download_categories() >> \
    unzip_archive >> repair_encoding >> [map_categories(), map_customs_log()] \
    >> reduce() >> fill_db() >> set_new_relevant_date()


dag=Customs_Enrich()
