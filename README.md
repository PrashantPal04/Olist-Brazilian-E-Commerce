# E-Commerce Sales Analytics
### Olist Brazilian E-Commerce | PostgreSQL + Power BI

### Page 1 — Executive Overview
<img width="2386" height="1341" alt="image" src="https://github.com/user-attachments/assets/a43a3c60-a580-4a12-8e15-68860d80c7da" />

### Page 2 — Customer Intelligence
<img width="2392" height="1354" alt="image" src="https://github.com/user-attachments/assets/38121e51-c2d6-4bda-a2be-4a3c501697e2" />

---

## Business Problem (SCQA)

**Situation:** Olist is a Brazilian e-commerce platform with 100K+ orders across multiple product categories and geographic regions (2016–2018).

**Complication:** Despite having raw transaction data, the business lacked a unified analytics infrastructure to systematically analyze performance across products, customers, and geographies.

**Question:** Which products, regions, and customer segments should the business prioritize to drive sustainable revenue growth and improve retention?

**Answer:** Built a star schema data warehouse in PostgreSQL, performed end-to-end EDA, calculated key KPIs, and implemented RFM segmentation — surfacing actionable insights across all business dimensions.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| PostgreSQL | Data warehouse, SQL analysis |
| Tableau Public | Interactive dashboards |
| SQL (CTEs, Window Functions) | EDA, KPIs, Segmentation |

---

## Dataset

- **Source:** [Olist Brazilian E-Commerce — Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Size:** 100K+ orders, 9 relational CSV files
- **Period:** 2016–2018

---

## Project Architecture

```
9 CSV Files (Kaggle)
      ↓
Staging Tables (temp_*)
      ↓
Star Schema (PostgreSQL)
      ↓
SQL Analysis + KPIs
      ↓
Tableau Dashboard
```

### Star Schema Design

```
              [dim_customers]
              customer_key PK
              customer_unique_id
              city, state
                    |
[dim_products]---[fact_sales]---[dim_time]
product_key PK   sale_id PK     time_key PK
product_id       customer_key   full_date
category_english product_key    month, quarter
                 geography_key  year, is_weekend
                 time_key
                 price
                 total_amount
                 order_status
                    |
              [dim_geography]
              geography_key PK
              zip_code
              city, state
```

---

## Key Findings

### Revenue & Growth
| Metric | 2017 | 2018 | Change |
|---|---|---|---|
| GMV | R$6,996,523 | R$8,532,429 | +22% |
| Net Revenue | R$6,783,977 | R$8,344,126 | +23% |
| Total Orders | 49,810 | 60,549 | +21% |

### Customer Metrics
| Metric | Value |
|---|---|
| Average LTV | R$160.70 |
| Overall Retention Rate | 9.93% |
| Unique Customers | 96,988 |
| Repeat Customers | 9,632 (10%) |

### Geographic Insights
- **SP (São Paulo)** dominates with **37.48%** of total revenue
- Top 3 states (SP, RJ, MG) contribute **62.7%** of revenue
- High concentration risk — 24 states share remaining 37%

### Category Insights
| Category | Revenue | AOV | YoY Growth |
|---|---|---|---|
| health_beauty | R$1.4M | R$149 | +60% |
| watches_gifts | R$1.3M | R$218 | +45% |
| computers | R$231K | R$1,145 | High AOV |

### RFM Segmentation (96K customers)
| Segment | Count | % |
|---|---|---|
| About to Sleep | 33,778 | 36% |
| Can't Lose Them | 15,093 | 16% |
| New Customers | 15,061 | 16% |
| Lost Customers | 14,913 | 16% |
| Loyal Customers | 7,493 | 8% |
| Champions | 7,492 | 8% |

---

## Business Recommendations

1. **Retention Crisis** — 90% customers are one-time buyers. Implement loyalty program targeting "About to Sleep" (33K customers) with re-engagement campaigns.

2. **Geographic Expansion** — Reduce SP dependency (37%). Invest in marketing for underserved states (RR, AP, AC — combined < 0.3% revenue).

3. **High-Value Categories** — Computers have highest AOV (R$1,145) but only 202 orders. Targeted campaigns could 2x revenue with minimal volume increase.

4. **Black Friday Strategy** — November spike driven by watches/gifts. Pre-stock and campaign 4 weeks early for maximum impact.

5. **Champion Retention** — 7,492 Champions drive disproportionate revenue. Exclusive early access and loyalty rewards to prevent churn.

---

## How to Run

### Prerequisites
- PostgreSQL installed
- pgAdmin or DBeaver
- Olist dataset from Kaggle

### Steps

```bash
# 1. Database banao
CREATE DATABASE ecommerce_analytics;

# 2. SQL file run karo
psql -U postgres -d ecommerce_analytics -f ecommerce_analysis.sql

# 3. CSV paths update karo Section 1 mein
# '/your/path/' ko apne actual path se replace karo

# 4. Sections 2-6 run karo analysis ke liye
```

---

## SQL Concepts Used

| Concept | Used For |
|---|---|
| Star Schema Design | Data modeling |
| CTEs (WITH clause) | RFM, Retention queries |
| Window Functions (NTILE, OVER) | RFM scoring, % share |
| GENERATE_SERIES | dim_time population |
| Multi-table JOINs | fact_sales loading |
| CASE WHEN | Segmentation logic |
| Aggregate Functions | KPI calculations |

---

## Author

**Prashant Pal**
B.Tech (ECE) | Data Analyst | QA Enthusiast
Skills: Python, SQL, Power BI, PostgreSQL, Excel
