import pandas as pd
from playwright.sync_api import sync_playwright
import time

def scrape_jumia_laptops(pages_to_scrape=1):
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
        page = context.new_page()
        
        base_url = "https://www.jumia.ma/ordinateurs-pc/"

        for page_num in range(1, pages_to_scrape + 1):
            url = f"{base_url}?page={page_num}"
            print(f"🔄 Scraping {url}...")
            page.goto(url, timeout=60000)
            page.wait_for_selector("article.prd", timeout=20000)
            page.wait_for_timeout(3000)

            # Get product elements
            items = page.query_selector_all("article.prd")
            prices = page.query_selector_all("div.prc")

            print(f"📦 Found {len(items)} products")
            print(f"💰 Found {len(prices)} prices")

            # Ensure equal length, or trim to shortest
            count = min(len(items), len(prices))

            for i in range(count):
                item = items[i]
                price_element = prices[i]

                name = item.query_selector("h3.name")
                link = item.query_selector("a.core")

                name_text = name.inner_text().strip() if name else "N/A"
                price_text = price_element.inner_text().strip().replace("MAD", "").replace("Dhs", "").replace(",", "").replace(" ", "")
                url = "https://www.jumia.ma" + link.get_attribute("href") if link else "N/A"

                print(f"✅ [{i+1}] {name_text} — {price_text} MAD")

                products.append({
                    "Product": name_text,
                    "Price": float(price_text) if price_text.replace('.', '').isdigit() else 0.0,
                    "URL": url,
                    "Platform": "Jumia"
                })

            time.sleep(2)  # polite delay

        browser.close()

    df = pd.DataFrame(products)
    df.to_csv("jumia_laptops.csv", index=False)
    print("✅ Data saved to 'jumia_laptops.csv'")

if __name__ == "__main__":
    scrape_jumia_laptops(pages_to_scrape=1)
