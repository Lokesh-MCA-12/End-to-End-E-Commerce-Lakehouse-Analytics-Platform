"""
Synthetic E-Commerce Dataset Generator
Generates realistic multi-source datasets for the E-Commerce Lakehouse Analytics Platform:
- Customers (CSV)
- Products (JSON)
- Orders (CSV)
- Order Items (CSV)
- Payments (JSON)
- Returns (CSV)
- Inventory (CSV)
"""

import os
import csv
import json
import random
from datetime import datetime, timedelta

def generate_ecommerce_data(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    random.seed(42)
    
    # 1. Generate Customers
    customers = []
    cities_states = [
        ("New York", "NY", "USA"), ("Los Angeles", "CA", "USA"), ("Chicago", "IL", "USA"),
        ("Houston", "TX", "USA"), ("Phoenix", "AZ", "USA"), ("Philadelphia", "PA", "USA"),
        ("San Antonio", "TX", "USA"), ("San Diego", "CA", "USA"), ("Dallas", "TX", "USA"),
        ("San Jose", "CA", "USA"), ("London", "ENG", "UK"), ("Toronto", "ON", "Canada")
    ]
    segments = ["Consumer", "Corporate", "Home Office", "VIP"]
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez"]
    
    start_date = datetime(2025, 1, 1)
    
    start_date = datetime(2025, 1, 1)
    
    for i in range(1, 501):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        city, state, country = random.choice(cities_states)
        reg_date = (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
        c = {
            "customer_id": f"CUST-{i:05d}",
            "first_name": fn,
            "last_name": ln,
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
            "gender": random.choice(["Male", "Female"]),
            "city": city,
            "state": state,
            "country": country,
            "registration_date": reg_date,
            "customer_segment": random.choice(segments)
        }
        customers.append(c)
        
    customers_file = os.path.join(output_dir, "raw_customers.csv")
    with open(customers_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)
        
    # 2. Generate Products (50 Product Catalog Items)
    categories = [
        (101, "Electronics", "TechCorp"),
        (102, "Apparel", "FashionHub"),
        (103, "Home & Kitchen", "HomePlus"),
        (104, "Beauty & Care", "GlowLab"),
        (105, "Sports & Fitness", "FitActive")
    ]
    
    products = []
    prod_names = [
        ("Wireless Noise-Canceling Headphones", 101, 199.99, 110.00),
        ("Ultra-HD Smart Monitor 27-inch", 101, 349.99, 210.00),
        ("Mechanical Gaming Keyboard", 101, 89.99, 45.00),
        ("Ergonomic Bluetooth Mouse", 101, 39.99, 18.00),
        ("Smart Watch Series X", 101, 279.99, 150.00),
        ("Portable Bluetooth Speaker", 101, 59.99, 25.00),
        ("HD Webcam 1080p", 101, 49.99, 20.00),
        ("Wireless Charging Pad", 101, 29.99, 10.00),
        ("Noise Isolating Earbuds", 101, 34.99, 12.00),
        ("USB-C Hub Multiport Adapter", 101, 44.99, 16.00),
        
        ("Organic Cotton Crewneck T-Shirt", 102, 24.99, 8.50),
        ("Slim Fit Denim Jeans", 102, 59.99, 22.00),
        ("Waterproof Winter Jacket", 102, 129.99, 55.00),
        ("Classic Leather Belt", 102, 34.99, 12.00),
        ("Running Performance Shorts", 102, 29.99, 10.00),
        ("Merino Wool Sweater", 102, 79.99, 30.00),
        ("Casual Canvas Sneakers", 102, 64.99, 25.00),
        ("Breathable Athletic Socks (3-Pack)", 102, 14.99, 4.00),
        ("Formal Dress Shirt", 102, 49.99, 18.00),
        ("Polarized Sunglasses", 102, 39.99, 12.00),

        ("Stainless Steel Espresso Machine", 103, 299.99, 160.00),
        ("Non-Stick Ceramic Cookware Set", 103, 149.99, 70.00),
        ("Robot Vacuum Cleaner", 103, 249.99, 120.00),
        ("High-Speed Countertop Blender", 103, 99.99, 42.00),
        ("Air Fryer Max XL 5.8-Quart", 103, 119.99, 55.00),
        ("Electric Programmable Kettle", 103, 39.99, 15.00),
        ("Memory Foam Pillows (Set of 2)", 103, 49.99, 18.00),
        ("Smart LED Desk Lamp", 103, 34.99, 12.00),
        ("French Press Coffee Maker", 103, 27.99, 9.00),
        ("Automatic Bread Maker Machine", 103, 139.99, 65.00),

        ("Hydrating Facial Serum", 104, 34.99, 10.00),
        ("Sunscreen SPF 50 Broad Spectrum", 104, 19.99, 5.50),
        ("Professional Hair Dryer", 104, 79.99, 32.00),
        ("Vitamin C Brightening Moisturizer", 104, 28.99, 8.00),
        ("Sonic Electric Toothbrush", 104, 59.99, 22.00),
        ("Organic Argan Hair Oil", 104, 22.99, 6.00),
        ("Exfoliating Scrub Lotion", 104, 18.99, 4.50),
        ("Luxury Cologne & Fragrance Spray", 104, 89.99, 35.00),
        ("Revitalizing Eye Cream", 104, 32.99, 9.00),
        ("Natural Clay Face Mask", 104, 21.99, 5.00),

        ("Non-Slip Yoga Mat with Strap", 105, 29.99, 11.00),
        ("Adjustable Dumbbell Set 50lbs", 105, 189.99, 95.00),
        ("Insulated Stainless Water Bottle", 105, 24.99, 7.00),
        ("Resistance Exercise Bands Set", 105, 19.99, 5.00),
        ("Trail Running Hydration Backpack", 105, 69.99, 28.00),
        ("GPS Bike Computer Tracker", 105, 149.99, 68.00),
        ("Padded Weightlifting Gloves", 105, 17.99, 4.50),
        ("High-Density Foam Roller", 105, 22.99, 6.50),
        ("Digital Jump Rope with Counter", 105, 16.99, 4.00),
        ("Compact Folding Treadmill", 105, 449.99, 220.00)
    ]
    
    for idx, (pname, cat_id, u_price, c_price) in enumerate(prod_names, 1):
        cat_name, supplier = [(name, supp) for cid, name, supp in categories if cid == cat_id][0]
        p = {
            "product_id": f"PROD-{idx:04d}",
            "product_name": pname,
            "category_id": cat_id,
            "category_name": cat_name,
            "brand": pname.split()[0],
            "unit_price": u_price,
            "cost_price": c_price,
            "supplier": supplier
        }
        products.append(p)
        
    products_file = os.path.join(output_dir, "raw_products.json")
    with open(products_file, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2)
        
    # 3. Generate Orders & Order Items (1,250 Total Orders)
    orders = []
    order_items = []
    payments = []
    returns = []
    
    statuses = ["Completed", "Completed", "Completed", "Completed", "Processing", "Shipped", "Cancelled"]
    pay_methods = ["Credit Card", "PayPal", "Apple Pay", "Debit Card"]
    pay_statuses = ["Success", "Success", "Success", "Success", "Failed"]
    return_reasons = ["Defective item", "Wrong size", "Changed mind", "Item not as described"]
    
    item_id_counter = 1
    payment_id_counter = 1
    return_id_counter = 1
    
    for o_idx in range(1, 1251):
        cust = random.choice(customers)
        o_date = start_date + timedelta(days=random.randint(30, 360))
        o_date_str = o_date.strftime("%Y-%m-%d %H:%M:%S")
        status = random.choice(statuses)
        order_id = f"ORD-{o_idx:06d}"
        
        # Order Items for this order
        num_items = random.randint(1, 4)
        selected_prods = random.sample(products, num_items)
        order_total = 0.0
        
        for prod in selected_prods:
            qty = random.randint(1, 3)
            u_price = prod["unit_price"]
            disc = round(random.choice([0.0, 0.0, 0.05, 0.10]) * u_price * qty, 2)
            item_total = (qty * u_price) - disc
            order_total += item_total
            
            oi = {
                "order_item_id": f"ITEM-{item_id_counter:06d}",
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": u_price,
                "discount": disc
            }
            order_items.append(oi)
            item_id_counter += 1
            
            # Returns (approx 8% return rate on completed orders)
            if status == "Completed" and random.random() < 0.08:
                r_date = (o_date + timedelta(days=random.randint(2, 14))).strftime("%Y-%m-%d")
                ret = {
                    "return_id": f"RET-{return_id_counter:05d}",
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "return_date": r_date,
                    "return_reason": random.choice(return_reasons),
                    "refund_amount": round(item_total, 2)
                }
                returns.append(ret)
                return_id_counter += 1
                
        order_total = round(order_total, 2)
        pay_id = f"PAY-{payment_id_counter:06d}"
        payment_id_counter += 1
        
        o = {
            "order_id": order_id,
            "customer_id": cust["customer_id"],
            "order_date": o_date_str,
            "order_status": status,
            "shipping_address": f"{cust['city']}, {cust['state']}, {cust['country']}",
            "payment_id": pay_id,
            "total_amount": order_total
        }
        orders.append(o)
        
        # Payment record
        p_status = "Failed" if status == "Cancelled" else random.choice(pay_statuses)
        pay = {
            "payment_id": pay_id,
            "order_id": order_id,
            "payment_method": random.choice(pay_methods),
            "payment_status": p_status,
            "payment_amount": order_total,
            "payment_date": o_date_str
        }
        payments.append(pay)
        
    # Write Orders
    orders_file = os.path.join(output_dir, "raw_orders.csv")
    with open(orders_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)
        
    # Write Order Items
    order_items_file = os.path.join(output_dir, "raw_order_items.csv")
    with open(order_items_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=order_items[0].keys())
        writer.writeheader()
        writer.writerows(order_items)
        
    # Write Payments
    payments_file = os.path.join(output_dir, "raw_payments.json")
    with open(payments_file, "w", encoding="utf-8") as f:
        json.dump(payments, f, indent=2)
        
    # Write Returns
    returns_file = os.path.join(output_dir, "raw_returns.csv")
    with open(returns_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=returns[0].keys())
        writer.writeheader()
        writer.writerows(returns)
        
    # 4. Generate Inventory
    inventory = []
    warehouses = ["WH-EAST-01", "WH-WEST-02", "WH-CENTRAL-03"]
    inv_date = datetime(2026, 1, 1).strftime("%Y-%m-%d")
    inv_counter = 1
    
    for prod in products:
        for wh in warehouses:
            inv = {
                "inventory_id": f"INV-{inv_counter:05d}",
                "product_id": prod["product_id"],
                "warehouse_id": wh,
                "stock_quantity": random.randint(5, 250),
                "inventory_date": inv_date
            }
            inventory.append(inv)
            inv_counter += 1
            
    inventory_file = os.path.join(output_dir, "raw_inventory.csv")
    with open(inventory_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=inventory[0].keys())
        writer.writeheader()
        writer.writerows(inventory)
        
    print(f"Dataset generation complete! Created 7 entities in '{output_dir}':")
    print(f" - Customers: {len(customers)} records ({customers_file})")
    print(f" - Products: {len(products)} records ({products_file})")
    print(f" - Orders: {len(orders)} records ({orders_file})")
    print(f" - Order Items: {len(order_items)} records ({order_items_file})")
    print(f" - Payments: {len(payments)} records ({payments_file})")
    print(f" - Returns: {len(returns)} records ({returns_file})")
    print(f" - Inventory: {len(inventory)} records ({inventory_file})")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sample_dir = os.path.join(base_dir, "sample_raw_data")
    generate_ecommerce_data(sample_dir)
