# Semantic Analysis of Reviews For Pipeline:

<br>

The project focus on semantic analysis of Google reviews for specific commerce major (For here: Restaurants) which used Google-reviews-scraper-pro project <br>
from Github. The project tried several models (Arabic/English) and (Base/Fine-Tune), and final results of search and test gathered into Apache-Airflow pipeline.

<br>

**Goal of Project:**
- Create a simple tool that help business decisions based on customer reviews in Google Map, for each last 35 days. 

<br>

**The Transformer Models Used:**
- CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment -> Arabic semantic (chosen).
- cardiffnlp/twitter-roberta-base-sentiment-latest -> English semantic (chosen).
- lxyuan/distilbert-base-multilingual-cased-sentiments-student -> English semantic.

<br>

**Google-Review-Scraper Repository:**
- [Repository](https://github.com/georgekhananaev/google-reviews-scraper-pro/tree/master)

<br>

**Search Links About Models:**
-[Choose the highest model types for semantic analysis](https://www.mdpi.com/1099-4300/27/12/1202)
- [CAMeL abilities](https://www.sciencedirect.com/org/science/article/pii/S1546221825006745)
- [TWITTER Roberta - understand unmeaningful texts like tweets](https://arxiv.org/abs/2010.12421)
- [DistilBERT - cheaper and lighter model than Roberta](https://arxiv.org/abs/1910.01108)

<br>

---

# Project Steps & Strategy:

<br>

1. Gather data reviews.
2. Explore and test models, including fine-tune them.
3. Create Apache-Airflow pipeline to do semantic analysis monthly.

<br>

*Strategy:* The project focus on test abilities of semantic analysis of models, what the results if fine-tune them, and how to choose the best <br>
options for business solutions. 

<br>

---

# Data Used in Project:

<br>

All Datasets found into `data` folder.
<br>

**Some Kaggle Datasets Used:**
- [English reviews](https://www.kaggle.com/datasets/joebeachcapital/restaurant-reviews)
- [Arabic reviews](https://www.kaggle.com/datasets/saudidata2030/jeddah-restaurants-review)
- [Arabic reviews2](https://www.kaggle.com/datasets/moazeldsokyx/arabic-resturant-reviews)

<br>

`Note:` Almost of datasets modified using AI (Adding labels or Create fake reviews with labels).


<br>

---

# Tools Used:

<br>

- Kaggle: Get Some of Datasets.
- Google Colab: Explore and Test models.
- VScode: Environment of write `app.py` code.
- Docker: Build Container for use Apache-Airflow.
- Apache-Airflow: Create automatic pipeline for extract, analysis, load data (ETL).

<br>

---

# Docker:

<br>

- Create project folder to hold all required files + folder of [Google-Review-Scraper repository](https://github.com/georgekhananaev/google-reviews-scraper-pro/tree/master)
- Into Same folder, run PowerShell.
- Type in PowerShell: `Remove-Item alias:curl`
- Type in PowerShell: `curl -LfO "https://airflow.apache.org/docs/apache-airflow/3.1.6/docker-compose.yaml"`
- Type in PowerShell: `docker compose build`
- Type in PowerShell: `docker compose up -d`

<br>

---

# Photos of Pipeline Project Results:

<br>

- Apache-Airflow UI:

<br>

![Apache-Airflow UI](/img/photo1)

<br>

- The Extracted .CSV File of Semantic Analysis - If want take it into Power BI to create dashboard:

<br>

![.CSV file](/img/photo2)

<br>

- Final Result into DB:

<br>

![DB](/img/photo3)

<br>

- Simple Analysis Diagram of Reviews:

<br>

![Simple Diagram](/img/photo4)
