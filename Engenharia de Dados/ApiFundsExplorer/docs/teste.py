from playwright.sync_api import sync_playwright
import json

def scrape_ranking():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )

        context = browser.new_context()
        page = context.new_page()

        print("Abrindo página...")

        with page.expect_response(
            lambda r: "admin-ajax.php" in r.url and r.status == 200
        ) as response_info:

            page.goto("https://www.fundsexplorer.com.br/ranking")

        response = response_info.value
        data = response.json()

        with open("ranking.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print("Arquivo salvo com sucesso.")

        browser.close()

scrape_ranking()