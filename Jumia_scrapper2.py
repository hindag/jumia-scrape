import pandas as pd
from playwright.sync_api import sync_playwright
import time
from datetime import datetime
import re
import logging
import json
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping_log.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedJumiaScraper:
    """Enhanced Jumia scraper for comprehensive market analysis"""
    
    def __init__(self):
        self.products = []
        self.categories_scraped = {}
        self.scraping_stats = {
            'start_time': datetime.now(),
            'total_products': 0,
            'categories_processed': 0,
            'pages_scraped': 0,
            'errors': 0
        }
        
        # Base categories and their default page counts
        self.base_categories = {
            "ordinateurs-pc": {
                "name": "Laptops & Computers", 
                "default_pages": 4, # Changed from 'pages' to 'default_pages'
                "expected_products": 80
            },
            "telephones-smartphones": {
                "name": "Smartphones", 
                "default_pages": 4, 
                "expected_products": 80
            },
            "tv-home-cinema-lecteurs": {
                "name": "Televisions", 
                "default_pages": 3, 
                "expected_products": 60
            },
            "mlp-electromenager": {
                "name": "Home Appliances", 
                "default_pages": 3, 
                "expected_products": 60
            },
            "jeux-videos-consoles": {
                "name": "Gaming", 
                "default_pages": 2, 
                "expected_products": 40
            }
        }
        
        # Common brands for better extraction
        self.known_brands = {
            'tech': ['HP', 'Dell', 'Lenovo', 'Asus', 'Acer', 'Apple', 'Microsoft', 'MSI'],
            'phones': ['Samsung', 'iPhone', 'Huawei', 'Xiaomi', 'OnePlus', 'Nokia', 'Oppo', 'Vivo'],
            'tv': ['Samsung', 'LG', 'Sony', 'TCL', 'Hisense', 'Toshiba'],
            'appliances': ['Samsung', 'LG', 'Whirlpool', 'Bosch', 'Electrolux'],
            'gaming': ['PlayStation', 'Xbox', 'Nintendo', 'Razer', 'Logitech']
        }
    
    # --- MODIFIED: Added max_pages_per_category argument ---
    def scrape_all_categories(self, max_pages_per_category=None):
        """
        Main scraping method for all categories.
        
        :param max_pages_per_category: If an integer, overrides the default 'pages' 
                                       for all categories. If None, uses default pages.
        """
        logger.info("🚀 Starting enhanced Jumia scraping")
        
        # Dynamically set categories to scrape based on the argument
        categories_to_scrape = {}
        for key, info in self.base_categories.items():
            pages = info['default_pages']
            if isinstance(max_pages_per_category, int) and max_pages_per_category >= 1:
                pages = max_pages_per_category # Override with the provided value
            
            categories_to_scrape[key] = {
                "name": info["name"], 
                "pages": pages, # Use 'pages' key for scraping logic
                "expected_products": info["expected_products"] * (pages / info["default_pages"])
            }
        
        self.categories = categories_to_scrape # Store the active list of categories
        
        logger.info(f"Categories to scrape: {list(self.categories.keys())}")
        logger.info(f"Page limit set to: {max_pages_per_category if max_pages_per_category is not None else 'Default'}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,  # Set to True for faster scraping
                slow_mo=100
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            page.set_default_timeout(30000)
            
            # Scrape each category using the active category list
            for category_key, category_info in self.categories.items():
                try:
                    self.scrape_category(page, category_key, category_info)
                    self.scraping_stats['categories_processed'] += 1
                    
                    # Log progress
                    logger.info(f"✅ Completed {category_info['name']}: {self.categories_scraped[category_key]['products_found']} products")
                    
                    # Delay between categories
                    time.sleep(5)
                    
                except Exception as e:
                    logger.error(f"❌ Error scraping {category_key}: {str(e)}")
                    self.scraping_stats['errors'] += 1
            
            browser.close()
        
        # Finalize and save data
        self.finalize_scraping()
        return self.save_data()
    
    def scrape_category(self, page, category_key, category_info):
        """Scrape a specific category"""
        category_products = []
        category_name = category_info['name']
        pages_to_scrape = category_info['pages'] # This now holds the dynamic/default page count
        
        logger.info(f"📦 Starting {category_name} ({pages_to_scrape} pages)")
        
        for page_num in range(1, pages_to_scrape + 1):
            try:
                url = f"https://www.jumia.ma/{category_key}/?page={page_num}"
                logger.info(f"🔄 Scraping page {page_num}: {url}")
                
                # Navigate to page
                page.goto(url, wait_until='domcontentloaded', timeout=60000)
                page.wait_for_selector("article.prd", timeout=20000)
                time.sleep(3)
                
                # Get product elements
                product_containers = page.query_selector_all("article.prd")
                logger.info(f"Found {len(product_containers)} products on page {page_num}")
                
                if not product_containers:
                    logger.warning(f"No products found on page {page_num} - stopping category.")
                    break # Break out of the page loop for this category
                
                # Process each product
                page_products = self.process_products(product_containers, category_key, category_name)
                category_products.extend(page_products)
                
                self.scraping_stats['pages_scraped'] += 1
                time.sleep(2)  # Polite delay
                
            except Exception as e:
                logger.error(f"Error scraping page {page_num} of {category_key}: {str(e)}")
                self.scraping_stats['errors'] += 1
                continue
        
        # Store category results
        self.categories_scraped[category_key] = {
            'name': category_name,
            'products_found': len(category_products),
            'pages_scraped': page_num # Use the last successfully scraped page number
        }
        
        self.products.extend(category_products)
        self.scraping_stats['total_products'] = len(self.products)
    
    def process_products(self, product_containers, category_key, category_name):
        """Process individual products from containers"""
        page_products = []
        
        for i, container in enumerate(product_containers):
            try:
                # Calculate product ID based on current total products + products on this page
                current_product_count = len(self.products) + len(page_products)
                product_data = self.extract_product_data(container, category_key, category_name, current_product_count + 1)
                
                if product_data:
                    page_products.append(product_data)
                    logger.info(f"✅ [{current_product_count + 1}] {product_data['product_name'][:50]}... - {product_data['current_price']} MAD")
                
            except Exception as e:
                logger.error(f"Error processing product {i+1}: {str(e)}")
                self.scraping_stats['errors'] += 1
                continue
        
        return page_products
    
    def extract_product_data(self, container, category_key, category_name, product_index):
        """Extract comprehensive product data"""
        try:
            # Basic elements
            name_elem = container.query_selector("h3.name")
            price_elem = container.query_selector("div.prc")
            old_price_elem = container.query_selector("div.old")
            link_elem = container.query_selector("a.core")
            # rating_elem = container.query_selector("div.stars") # unused
            # reviews_elem = container.query_selector("div.rev") # unused
            
            # Extract basic data
            product_name = name_elem.inner_text().strip() if name_elem else "N/A"
            current_price = self.clean_price(price_elem.inner_text() if price_elem else "0")
            original_price = self.clean_price(old_price_elem.inner_text() if old_price_elem else "0")
            
            # Skip if no valid price
            if current_price == 0:
                return None
            
            # Extract additional data
            brand = self.extract_brand(product_name, category_key)
            model = self.extract_model(product_name, brand)
            discount_percent = self.calculate_discount(current_price, original_price)
            
            # Build product URL
            product_url = ""
            if link_elem:
                href = link_elem.get_attribute("href")
                product_url = f"https://www.jumia.ma{href}" if href else ""
            
            # Calculate derived metrics
            price_tier = self.classify_price_tier(current_price, category_key)
            value_score = self.calculate_value_score(current_price, original_price, discount_percent)
            
            # Create product data
            product_data = {
                "product_id": f"JUM_{product_index:04d}", # Use passed index
                "product_name": product_name,
                "brand": brand,
                "model": model,
                "category": category_name,
                "category_key": category_key,
                "current_price": current_price,
                "original_price": original_price if original_price > 0 else None,
                "discount_percent": discount_percent,
                "price_tier": price_tier,
                "value_score": value_score,
                "is_on_sale": discount_percent > 0,
                "url": product_url,
                "scraped_date": datetime.now().date(),
                "scraped_time": datetime.now().strftime("%H:%M:%S"),
                "scraped_timestamp": datetime.now().isoformat()
            }
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error extracting product data: {str(e)}")
            return None
    
    def clean_price(self, price_text):
        """Clean and convert price to float"""
        if not price_text:
            return 0.0
        
        # Remove everything except digits, commas, and dots
        cleaned = re.sub(r'[^\d,.]', '', price_text.strip())
        cleaned = cleaned.replace(',', '')
        
        try:
            return float(cleaned) if cleaned else 0.0
        except ValueError:
            return 0.0
    
    def extract_brand(self, product_name, category_key):
        """Extract brand from product name based on category"""
        product_upper = product_name.upper()
        
        # Get relevant brands for category
        if category_key in ['ordinateurs-pc', 'jeux-videos-consoles']:
            brands_to_check = self.known_brands['tech'] + self.known_brands['gaming']
        elif category_key == 'telephones-smartphones':
            brands_to_check = self.known_brands['phones']
        elif category_key == 'tv-home-cinema-lecteurs':
            brands_to_check = self.known_brands['tv']
        elif category_key == 'mlp-electromenager':
            brands_to_check = self.known_brands['appliances']
        else:
            # Fallback for unexpected categories
            brands_to_check = []
            for brand_list in self.known_brands.values():
                brands_to_check.extend(brand_list)
        
        # Check for known brands
        for brand in brands_to_check:
            if brand.upper() in product_upper:
                return brand
        
        # Fallback: use first word
        first_word = product_name.split()[0] if product_name.split() else "Unknown"
        return first_word[:20]  # Limit length
    
    def extract_model(self, product_name, brand):
        """Extract model information"""
        if brand == "Unknown" or brand not in product_name:
            return product_name[:100]
        
        # Remove brand and clean up
        model = product_name.replace(brand, "").strip()
        # Remove common words
        common_words = ['Laptop', 'Smartphone', 'TV', 'Inch', 'GB', 'TB', 'RAM', 'SSD', 'HDD']
        for word in common_words:
            model = model.replace(word, "").replace(word.upper(), "").replace(word.lower(), "")
        
        return model.strip()[:100]
    
    def calculate_discount(self, current_price, original_price):
        """Calculate discount percentage"""
        if not original_price or original_price <= current_price:
            return 0.0
        return round(((original_price - current_price) / original_price) * 100, 2)
    
    def classify_price_tier(self, price, category_key):
        """Classify price into tiers based on category"""
        price_ranges = {
            "ordinateurs-pc": {"budget": 3000, "premium": 10000},
            "telephones-smartphones": {"budget": 2000, "premium": 6000},
            "tv-home-cinema-lecteurs": {"budget": 2500, "premium": 8000},
            "mlp-electromenager": {"budget": 1500, "premium": 5000},
            "jeux-videos-consoles": {"budget": 1000, "premium": 4000}
        }
        
        # Use key to look up ranges, falling back to a general range
        ranges = price_ranges.get(category_key, {"budget": 1000, "premium": 5000})
        
        if price <= ranges["budget"]:
            return "Budget"
        elif price <= ranges["premium"]:
            return "Mid-range"
        else:
            return "Premium"
    
    def calculate_value_score(self, current_price, original_price, discount_percent):
        """Calculate value score (0-100)"""
        base_score = 50
        
        # Discount bonus (0-30 points)
        discount_bonus = min(discount_percent * 0.6, 30)
        
        # Price factor (lower price = higher score for value)
        if current_price < 1000:
            price_bonus = 20
        elif current_price < 3000:
            price_bonus = 10
        elif current_price < 5000:
            price_bonus = 0
        else:
            price_bonus = -10
        
        total_score = base_score + discount_bonus + price_bonus
        return round(max(0, min(100, total_score)), 2)
    
    def finalize_scraping(self):
        """Finalize scraping statistics"""
        self.scraping_stats['end_time'] = datetime.now()
        self.scraping_stats['duration'] = str(self.scraping_stats['end_time'] - self.scraping_stats['start_time'])
        self.scraping_stats['total_products'] = len(self.products)
        
        logger.info("📊 SCRAPING COMPLETED!")
        logger.info(f"Total products: {self.scraping_stats['total_products']}")
        logger.info(f"Categories processed: {self.scraping_stats['categories_processed']}")
        logger.info(f"Pages scraped: {self.scraping_stats['pages_scraped']}")
        logger.info(f"Duration: {self.scraping_stats['duration']}")
        logger.info(f"Errors: {self.scraping_stats['errors']}")
    
    def save_data(self):
        """Save data in multiple formats"""
        if not self.products:
            logger.warning("No products to save!")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Convert to DataFrame
        df = pd.DataFrame(self.products)
        
        # Add summary statistics
        df_summary = self.generate_summary_stats(df)
        
        # Save CSV
        csv_filename = f"jumia_enhanced_data_{timestamp}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        logger.info(f"💾 Saved CSV: {csv_filename}")
        
        # Save Excel with multiple sheets
        excel_filename = f"jumia_enhanced_data_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Products', index=False)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Category breakdown
            category_summary = df.groupby('category').agg({
                'product_id': 'count',
                'current_price': ['mean', 'min', 'max'],
                'discount_percent': 'mean',
                'value_score': 'mean'
            }).round(2)
            category_summary.to_excel(writer, sheet_name='Category_Analysis')
        
        logger.info(f"📊 Saved Excel: {excel_filename}")
        
        # Save statistics
        stats_filename = f"scraping_stats_{timestamp}.json"
        stats_data = {
            'scraping_stats': self.scraping_stats,
            'categories_scraped': self.categories_scraped,
            'data_summary': {
                'total_products': len(df),
                'categories': df['category'].nunique(),
                'brands': df['brand'].nunique(),
                'avg_price': float(df['current_price'].mean()),
                'price_range': [float(df['current_price'].min()), float(df['current_price'].max())],
                'products_on_sale': int(df['is_on_sale'].sum()),
                'avg_discount': float(df[df['discount_percent'] > 0]['discount_percent'].mean() or 0)
            }
        }
        
        with open(stats_filename, 'w', encoding='utf-8') as f:
            json.dump(stats_data, f, indent=2, default=str)
        
        logger.info(f"📈 Saved stats: {stats_filename}")
        
        return df, csv_filename, excel_filename
    
    def generate_summary_stats(self, df):
        """Generate summary statistics DataFrame"""
        summary_data = []
        
        # Overall stats
        summary_data.append({
            'Metric': 'Total Products',
            'Value': len(df),
            'Description': 'Total number of products scraped'
        })
        
        summary_data.append({
            'Metric': 'Categories',
            'Value': df['category'].nunique(),
            'Description': 'Number of different categories'
        })
        
        summary_data.append({
            'Metric': 'Brands',
            'Value': df['brand'].nunique(),
            'Description': 'Number of different brands'
        })
        
        summary_data.append({
            'Metric': 'Average Price (MAD)',
            'Value': round(df['current_price'].mean(), 2),
            'Description': 'Average product price'
        })
        
        summary_data.append({
            'Metric': 'Products on Sale',
            'Value': int(df['is_on_sale'].sum()),
            'Description': 'Number of products with discounts'
        })
        
        summary_data.append({
            'Metric': 'Average Discount (%)',
            'Value': round(df[df['discount_percent'] > 0]['discount_percent'].mean() or 0, 2),
            'Description': 'Average discount percentage (for discounted items)'
        })
        
        return pd.DataFrame(summary_data)

# Usage
if __name__ == "__main__":
    scraper = EnhancedJumiaScraper()
    
    # --- MODIFIED USAGE: Prompt user for the number of pages to scrape ---
    
    # Default is None, which uses the internal base_categories default_pages.
    # To run a full scrape, you can pass an arbitrarily high number like 100.
    # To run a quick test, you can pass 1.
    
    try:
        pages_input = input("Enter the number of pages to scrape per category (e.g., '1' for a quick test, 'None' for default pages): ")
        if pages_input.lower() == 'none':
            PAGES_TO_SCRAPE = None
        else:
            PAGES_TO_SCRAPE = int(pages_input)
            if PAGES_TO_SCRAPE < 1:
                PAGES_TO_SCRAPE = 1
                print("Minimum pages is 1. Using 1 page per category.")

    except ValueError:
        print("Invalid input. Using default pages per category.")
        PAGES_TO_SCRAPE = None
        
    # Calculate expected products based on the choice
    expected_products_total = 0
    for key, info in scraper.base_categories.items():
        pages = info['default_pages']
        if PAGES_TO_SCRAPE is not None:
            pages = PAGES_TO_SCRAPE
        expected_products_total += info['expected_products'] * (pages / info['default_pages'])

    print("\n🚀 Enhanced Jumia Scraper")
    print(f"Categories: {list(scraper.base_categories.keys())}")
    print(f"Page Limit per Category: {PAGES_TO_SCRAPE if PAGES_TO_SCRAPE is not None else 'Default'}")
    print(f"Expected products (approx.): {int(expected_products_total)}")
    print("\nStarting scraper...")
    
    # Run the scraper with the selected page count
    result = scraper.scrape_all_categories(max_pages_per_category=PAGES_TO_SCRAPE)
    
    if result:
        df, csv_file, excel_file = result
        print(f"\n✅ SUCCESS!")
        print(f"📊 Total products scraped: {len(df)}")
        print(f"💾 Files saved: {csv_file}, {excel_file}")
        print(f"🎯 Ready for analysis!")
    else:
        print("❌ Scraping failed - check logs")