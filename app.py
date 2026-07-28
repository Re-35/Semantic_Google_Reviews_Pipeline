from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from transformers import pipeline
from datasets import Dataset
from datetime import datetime, timedelta, date
from ruamel.yaml import YAML
import sqlite3
import pandas as pd
import json
import re

BASE_DIR = r'/opt/airflow/dags/google-reviews-scraper-pro'
YAML_FILE_PATH = f'{BASE_DIR}/config.yaml'
DB_PATH = f'{BASE_DIR}/reviews.db'

modelAr = 'CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment'
modelEn = 'cardiffnlp/twitter-roberta-base-sentiment-latest'


# TASK (1):
def update_config():
    res_date = (date.today() - timedelta(days=35)).strftime('%Y-%m-%d')

    yaml = YAML()
    yaml.preserve_quotes = True

    # read existing config:
    with open(file=YAML_FILE_PATH, mode='r' ,encoding='utf-8') as f:
        config = yaml.load(f)

    # add the new filter date:
    config['date_filter']['after'] = res_date

    # write updated config data into config.yaml
    with open(file=YAML_FILE_PATH, mode='w', encoding='utf-8') as f:
        yaml.dump(config, f)


    print(f'Updating config.yaml successfully by {res_date}')

# ------------------------------------------------------------

# Task (2):
# Run Google review scraper using BashOperator.

#-------------------------------------------------------------

# Task(3):

def load_and_analysis():
    # Load data from sql DB:
    conn = sqlite3.connect(DB_PATH)

    query = '''
    SELECT review_id, review_text, review_date
    FROM reviews
    '''

    df = pd.read_sql(query, conn)
    print(f"🔎 DEBUG: Loaded {len(df)} rows from the database.")

    # extract Arabic and English reviews separately:
    def extract_diff_lang(text):
        if not isinstance(text, str):
            return "", ""
        if text.startswith('{'):
            try:
                data = json.loads(text)
                return data.get('ar', ""), data.get('en', "")
            except:
                return text, ""
        return text, ""

    
    extracted_data = df['review_text'].apply(extract_diff_lang)
    df['ar_rev'] = extracted_data.apply(lambda x: x[0])
    df['en_rev'] = extracted_data.apply(lambda x: x[1])

    # separate different language reviews into 2 DFs:
    df_ar = df[['review_id','ar_rev', 'review_date']]
    df_en = df[['review_id','en_rev', 'review_date']]

    # drop null vlues:
    df_ar = df_ar.dropna()
    df_en = df_en.dropna()

    # clean texts:
    def clean_arabic(text):
        # remove any character nither string nor digit:
        text = re.sub(r'[^0-9\u0600-\u06FF\s]', '', text)
        # remove tashkeel symbols:
        text = re.sub(r'[\u064B-\u0652\u0653-\u0655\u0670]', '', text).strip()
        # remove newline:
        text = text.replace('\n', ' ').strip()

        return text


    def clean_english(text):
        # remove nither string nor digit:
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text).strip()
        # remove newline:
        text = text.replace('\n', ' ').strip()

        return text


    # apply cleaning text:
    df_ar['ar_rev'] = df_ar['ar_rev'].apply(clean_arabic)
    df_en['en_rev'] = df_en['en_rev'].apply(clean_english)

    df_ar = df_ar[df_ar['ar_rev'] != '']
    df_en = df_en[df_en['en_rev'] != '']
    print(f"🔎 DEBUG: After cleaning, AR has {len(df_ar)} rows, EN has {len(df_en)} rows.")


    # Load models:
    pipeline_ar = pipeline("sentiment-analysis", modelAr)
    pipeline_en = pipeline("sentiment-analysis", modelEn)

    # get results:
    res_ar = pipeline_ar(list(df_ar['ar_rev'].to_list())) if not df_ar.empty else []
    res_en = pipeline_en(list(df_en['en_rev'].to_list())) if not df_en.empty else []


    # extractLabels:
    def extractLabels(res_ar, res_en):
        label_ar = []
        score_ar = []
        label_en = []
        score_en = []

        for i in range(len(res_ar)):
            label_ar.append(res_ar[i]['label'])
            score_ar.append(round(res_ar[i]['score'], 2))

        for i in range(len(res_en)):
            label_en.append(res_en[i]['label'])
            score_en.append(round(res_en[i]['score'], 2))

        return label_ar, score_ar, label_en, score_en

    label_ar, score_ar, label_en, score_en = extractLabels(res_ar, res_en)

    # insert labels and score into DB and DFs:
    df_ar['label'] = label_ar
    df_ar['score'] = score_ar

    df_en['label'] = label_en
    df_en['score'] = score_en

    df_all = pd.concat([df_ar, df_en])
    df_all.to_csv(f'{BASE_DIR}/data_reviews.csv')
    print('.CSV file uploaded successfully')

    ar_updates = list(zip(df_ar['label'], df_ar['score'], df_ar['review_id']))
    en_updates = list(zip(df_en['label'], df_en['score'], df_en['review_id']))

    all_updates = ar_updates + en_updates

    cursor = conn.cursor()
    cursor.executemany("""
        UPDATE reviews 
        SET label = ?, score = ? 
        WHERE review_id = ?
        """, all_updates)

    conn.commit()
    conn.close()
    print('DB updated successfully')

#-----------------------------------------------------
# define Airflow DAG:
default_args = {
'owner': 'data_explorer',
'depends_on_past': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

with DAG('monthly_semantic_review_pipeline', default_args=default_args, 
        description='Scrapes Google Maps reviews and runs Arabic/English sentiment classification',
        schedule='@monthly',
        start_date=datetime(2026, 7, 27),
        catchup=False,
        tags=['nlp', 'reviews']
) as dag:

    # task (1)
    update_config_task = PythonOperator(task_id='update_config_yaml_task', python_callable=update_config)

    # task (2)
    scraper_task = BashOperator(task_id='scrape_reviews_task', bash_command=f'cd {BASE_DIR} && python start.py')

    # task (3)
    semantic_analysis_task = PythonOperator(task_id='load_data_and_semantic_analysis_task', python_callable=load_and_analysis)


    # pipeline excution order:
    update_config_task >> scraper_task >> semantic_analysis_task
