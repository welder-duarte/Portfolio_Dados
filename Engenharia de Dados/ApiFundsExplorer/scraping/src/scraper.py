import argparse, logging, time, warnings, pandas as pd, os
from datetime import date, datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from google.cloud import storage
from urllib.request import Request, urlopen
from webdriver_manager.chrome import ChromeDriverManager


# Configurações globais
URL = "https://www.fundsexplorer.com.br/ranking"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Funções auxiliares
def init_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    running_in_container = os.getenv("RUNNING_IN_CONTAINER", "false").lower() == "true"

    if running_in_container:
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.binary_location = os.getenv("CHROME_BIN", "/usr/bin/chromium")
        service = Service(os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver"))
    else:
        service = Service(ChromeDriverManager().install())
        
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def close_popups(driver: webdriver.Chrome) -> None:
    try:
        actions = ActionChains(driver)
        shadow_host = driver.find_element(By.ID, "wisepops-instance-572651")
        shadow_root = driver.execute_script("return arguments[0].shadowRoot", shadow_host)
        shadow_root.find_element(By.CSS_SELECTOR, "button").click()
        logging.info("Popup fechado (wisepops)")
    except Exception:
        pass

    try:
        shadow_host = driver.find_element(By.ID, "interactive-close-button")
        shadow_root = driver.execute_script("return arguments[0].shadowRoot", shadow_host)
        shadow_root.find_element(By.CSS_SELECTOR, "button").click()
        logging.info("Popup fechado (interactive)")
    except Exception:
        pass


def extract_columns() -> list[str]:
    html = urlopen(Request(URL, headers=HEADERS))
    soup = BeautifulSoup(html.read(), "html.parser")

    labels = soup.find_all("label", {"class": "checkbox-container"})
    columns = []

    for label in labels:
        input_el = label.find("input")
        if input_el and "colunas-ranking__selectItem" in input_el.get("class", []):
            columns.append(input_el.get("value").upper())

    # Remove a primeira coluna (checkbox)
    return columns[1:]


#Scraping
def scrape_funds() -> pd.DataFrame:
    warnings.filterwarnings("ignore")
    logging.info("Iniciando scraping do Funds Explorer")

    driver = init_driver()
    wait = WebDriverWait(driver, 20)

    driver.get(URL)
    time.sleep(10)

    close_popups(driver)

    # Abrir seletor de colunas
    select_button = wait.until(EC.element_to_be_clickable((By.ID, "colunas-ranking__select-button")))
    driver.execute_script("arguments[0].scrollIntoView(true);", select_button)
    driver.execute_script("""document.querySelectorAll('iframe, p, section').forEach(e => e.remove());""")
    driver.execute_script("arguments[0].click();", select_button)
    time.sleep(3)

    # Selecionar todas as colunas
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='colunas-ranking__select']/li[1]/label"))).click()

    # Coletar dados da tabela
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "default-fiis-table__container__table__body")))
    tbody = driver.find_element(By.CLASS_NAME, "default-fiis-table__container__table__body")
    rows = tbody.find_elements(By.TAG_NAME, "tr")

    table_data = []
    for row in rows:
        cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]
        table_data.append(cols)

    driver.quit()
    columns = extract_columns()
    df = pd.DataFrame(table_data, columns=columns)

    # Metadados de ingestão
    df["scraping_date"] = date.today()
    df["ingestion_timestamp"] = datetime.now()

    logging.info(f"Scraping finalizado com {len(df)} registros")
    return df

# CLI
def save_output(df: pd.DataFrame, output_path: str) -> None:
    df.to_csv(output_path, index=False)
    logging.info(f"Arquivo salvo em {output_path}")

def upload_to_gcs(local_path: str, bucket_name: str, destination_path: str):
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not cred_path or not os.path.isfile(cred_path):
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS não configurada corretamente "
            "ou não aponta para um arquivo JSON válido."
        )

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(local_path)

    logging.info(f"Upload concluído: gs://{bucket_name}/{destination_path}")



def main(output_path: str):
    df = scrape_funds()
    save_output(df, output_path)

    upload_to_gcs(
        local_path=output_path,
        bucket_name="funds-explorer-raw-apifundsexplorer",
        destination_path=(
            f"funds_explorer/"
            f"scraping_date={date.today()}/funds.csv"
        )
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraping diário do Funds Explorer")

    parser.add_argument(
        "--output",
        type=str,
        default=f"funds_{date.today()}.csv",
        help="Caminho do arquivo de saída"
    )

    args = parser.parse_args()
    main(args.output)