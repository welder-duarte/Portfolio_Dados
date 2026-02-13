import json, logging, pandas as pd, os
from datetime import date, datetime
from google.cloud import storage
from playwright.sync_api import sync_playwright

BUCKET_NAME = os.getenv("BUCKET_NAME", "seu-bucket")
DESTINATION_PREFIX = "raw/funds_ranking"
PROJECT_ID = os.getenv("PROJECT_ID") 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract_ranking():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True,args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context()
        page = context.new_page()

        with page.expect_response(lambda r: "admin-ajax.php" in r.url and r.status == 200) as response_info:
            page.goto("https://www.fundsexplorer.com.br/ranking")

        response = response_info.value
        data = response.json()
        browser.close()
        logging.info(f"Scraping finalizado")

        return data["data"]


def transform_to_dataframe(data):
    df = pd.DataFrame(data)

    df["scraping_date"] = date.today()
    df["ingestion_timestamp"] = datetime.now()
    logging.info(f"Dataframe criado com sucesso - {df.shape[0]} registros")
    return df


def upload_to_gcs(df):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    destination_path = (
        "raw/funds_explorer/"
        f"scraping_date_{date.today()}/"
        "funds.csv"
    )

    blob = bucket.blob(destination_path)
    csv_data = df.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type="text/csv")

    logging.info(f"Upload concluído: gs://{BUCKET_NAME}/{destination_path}")


def main():
    raw_data = extract_ranking()
    df = transform_to_dataframe(raw_data)
    upload_to_gcs(df)


if __name__ == "__main__":
    main()