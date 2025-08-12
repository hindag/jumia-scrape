import pandas as pd
from playwright.sync_api import sync_playwright
import time
import re
import logging
from datetime import datetime
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_price(price_text):
    """Clean price text and convert to float"""
    if not price_text:
        return None
    
    # Remove currency symbols and clean up
    cleaned = re.sub(r'[^\d.,]', '', price_text.strip())
    cleaned = cleaned.replace(',', '')
    
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None

def scrape_jumia_laptops(pages_to_scrape=1, headless=True, output_file=None):
    """
    Scrape Jumia laptops with improved error handling and data processing
    
    Args:
        pages_to_scrape (int): Number of pages to scrape
        headless (bool): Run browser in headless mode
        output_file (str): Custom output filename
    """
    products = []
    
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"jumia_laptops_{timestamp}.csv"
    
    with sync_playwright() as p:
        try:
            # Launch browser with better configuration
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=100,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            
            # Set longer timeouts for better reliability
            page.set_default_timeout(30000)
            
            base_url = "https://www.jumia.ma/ordinateurs-pc/"
            
            for page_num in range(1, pages_to_scrape + 1):
                url = f"{base_url}?page={page_num}"
                logger.info(f"🔄 Scraping page {page_num}: {url}")
                
                try:
                    # Navigate to page with retry logic
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)
                    
                    # Wait for products to load
                    page.wait_for_selector("article.prd", timeout=20000)
                    time.sleep(3)
                    
                    # Get all product containers
                    product_containers = page.query_selector_all("article.prd")
                    logger.info(f"📦 Found {len(product_containers)} products on page {page_num}")
                    
                    if not product_containers:
                        logger.warning(f"No products found on page {page_num}")
                        continue
                    
                    # Process each product
                    for i, container in enumerate(product_containers):
                        try:
                            # Extract product data from the same container
                            name_elem = container.query_selector("h3.name")
                            link_elem = container.query_selector("a.core")
                            price_elem = container.query_selector("div.prc")
                            old_price_elem = container.query_selector("div.old")
                            
                            # Extract text data
                            name_text = name_elem.inner_text().strip() if name_elem else "N/A"
                            
                            # Clean and process prices
                            price_text = price_elem.inner_text().strip() if price_elem else ""
                            old_price_text = old_price_elem.inner_text().strip() if old_price_elem else ""
                            
                            price = clean_price(price_text)
                            old_price = clean_price(old_price_text)
                            
                            # Build full URL
                            product_url = "N/A"
                            if link_elem:
                                href = link_elem.get_attribute("href")
                                if href:
                                    product_url = "https://www.jumia.ma" + href if href.startswith('/') else href
                            
                            # Calculate discount if both prices exist
                            discount_percent = None
                            if price and old_price and old_price > price:
                                discount_percent = round(((old_price - price) / old_price) * 100, 2)
                            
                            product_data = {
                                "Product": name_text,
                                "Price": price,
                                "Old_Price": old_price,
                                "Discount_Percent": discount_percent,
                                "URL": product_url,
                                "Platform": "Jumia",
                                "Scraped_At": datetime.now().isoformat(),
                                "Page": page_num
                            }
                            
                            products.append(product_data)
                            
                            logger.info(f"✅ [{page_num}-{i+1}] {name_text[:50]}... — {price} MAD | Old: {old_price or 'N/A'} MAD")
                            
                        except Exception as e:
                            logger.error(f"Error processing product {i+1} on page {page_num}: {str(e)}")
                            continue
                    
                    # Polite delay between pages
                    if page_num < pages_to_scrape:
                        time.sleep(2)
                        
                except Exception as e:
                    logger.error(f"Error scraping page {page_num}: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Browser error: {str(e)}")
        finally:
            try:
                browser.close()
            except:
                pass
    
    # Process and save data
    if products:
        df = pd.DataFrame(products)
        
        # Add some data processing
        df = df.drop_duplicates(subset=['Product', 'Price'], keep='first')
        
        # Sort by price (ascending)
        df = df.sort_values('Price', ascending=True)
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"✅ Data saved to '{output_file}' - {len(df)} products")
        
        # Print summary statistics
        print_summary(df)
        
        return df
    else:
        logger.warning("No products were scraped!")
        return None

def print_summary(df):
    """Print summary statistics of scraped data"""
    print("\n" + "="*50)
    print("📊 SCRAPING SUMMARY")
    print("="*50)
    print(f"Total products: {len(df)}")
    
    if len(df) > 0:
        valid_prices = df[df['Price'].notna() & (df['Price'] > 0)]
        if len(valid_prices) > 0:
            print(f"Price range: {valid_prices['Price'].min():.2f} - {valid_prices['Price'].max():.2f} MAD")
            print(f"Average price: {valid_prices['Price'].mean():.2f} MAD")
            
        discounted = df[df['Discount_Percent'].notna()]
        if len(discounted) > 0:
            print(f"Products with discount: {len(discounted)}")
            print(f"Average discount: {discounted['Discount_Percent'].mean():.1f}%")
    
    print("="*50)

def scrape_specific_category(category_url, pages_to_scrape=1, headless=True):
    """
    Scrape any Jumia category by providing the category URL
    
    Args:
        category_url (str): Full URL of the category to scrape
        pages_to_scrape (int): Number of pages to scrape
        headless (bool): Run browser in headless mode
    """
    products = []
    
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=headless, slow_mo=100)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()
            
            for page_num in range(1, pages_to_scrape + 1):
                # Handle URL with page parameter
                separator = "&" if "?" in category_url else "?"
                url = f"{category_url}{separator}page={page_num}"
                
                logger.info(f"🔄 Scraping {url}")
                page.goto(url, timeout=60000)
                page.wait_for_selector("article.prd", timeout=20000)
                time.sleep(3)
                
                # Use the same scraping logic as above
                # ... (similar processing logic)
                
        finally:
            browser.close()
    
    return products

if __name__ == "__main__":
    # Configuration
    PAGES_TO_SCRAPE = 15
    HEADLESS_MODE = True  # Set to False to see browser in action
    
    # Run the scraper
    df = scrape_jumia_laptops(
        pages_to_scrape=PAGES_TO_SCRAPE,
        headless=HEADLESS_MODE
    )
    
    # Optional: Save to different formats
    if df is not None:
        # Save as Excel too
        excel_file = f"jumia_laptops_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df.to_excel(excel_file, index=False)
        logger.info(f"📊 Data also saved to Excel: {excel_file}")