import pandas as pd
from playwright.sync_api import sync_playwright
import time

def scrape_jumia_laptops(pages_to_scrape=3):
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        # browser = p.chromium.launch(headless=True, slow_mo=100)
        page = browser.new_page()
        page.set_default_timeout(60000)  # Set default timeout to 60 seconds
        page.wait_for_selector("h3.name", timeout=20000)

        base_url = "https://www.jumia.ma/ordinateurs-pc/"

        for page_num in range(1, pages_to_scrape + 1):
            url = f"{base_url}?page={page_num}"
            print(f"Scraping {url}...")
            page.goto(url, timeout=60000)
            page.wait_for_selector("article.prd", timeout=20000)

            # ✅ this defines 'items'
            page.screenshot(path=f"page_debug_{page_num}.png", full_page=True)
            print(f"✅ Page {page_num} loaded successfully.")
            # Get all product items on the page
            items = page.query_selector_all("article.prd")

            for item in items:
                print("========= ITEM START =========")
                print(item.inner_html())  
                print("========= ITEM END ===========")
                name = item.query_selector("h3.name")
                if not name:
                    print("❌ No name found for item!")
                else:
                    print("✅ Name found:", name.inner_text())
                    name = item.query_selector("h3.name")
                price = item.query_selector("div.prc")
                if not price:
                    print("❌ No price found for item!")
                else:
                    print("✅ price found:", price.inner_text())
                old_price = item.query_selector("div.old")
                if not old_price:
                    print("❌ No old price found for item!")
                else:
                    print("✅ Old price found:", old_price.inner_text())

                link = item.query_selector("a.core")
                if not link:
                    print("❌ No link found for item!")
                else:
                    print("✅ Link found:", link.get_attribute("href"))
                rating = item.query_selector("div.rev div.stars._s")

                if rating:
                    rating_raw = rating.get_attribute("aria-label")
                    rating_text = rating_raw.strip() if rating_raw else "N/A"
                else:
                    rating_text = "N/A"

                name_text = name.inner_text().strip() if name else "N/A"
                price_text = price.inner_text().strip().replace("MAD", "").replace(",", "").replace(" ", "") if price else "0"
                old_price_text = old_price.inner_text().strip().replace("MAD", "").replace(",", "").replace(" ", "") if old_price else "0"
                link_url = "https://www.jumia.ma" + link.get_attribute("href") if link else "N/A"

                products.append({
                    "Product": name_text,
                    "Current Price": price_text,
                    "Old Price": old_price_text,
                    "Rating": rating_text,
                    "URL": link_url,
                    "Platform": "Jumia"
                })
                print("========== PRODUCT HTML ==========")
                print(item.inner_html())
                print("==================================")


            time.sleep(5)  # polite delay

        browser.close()

    # Convert prices to float
    for product in products:
        try:
            product["Current Price"] = float(product["Current Price"])
        except:
            product["Current Price"] = 0.0
        try:
            product["Old Price"] = float(product["Old Price"])
        except:
            product["Old Price"] = 0.0

    df = pd.DataFrame(products)
    df.to_csv("jumia_laptops_v2.csv", index=False)
    print("✅ Data saved to 'jumia_laptops.csv'")

if __name__ == "__main__":
    scrape_jumia_laptops(pages_to_scrape=1)
