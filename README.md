# Product Deduplication System

This repository contains a Python-based system for identifying and managing duplicate product entries in a database. The system processes product data daily, providing merge suggestions for similar products to enhance data quality and reduce redundancy. 

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
  - [Initial Setup](#initial-setup)
  - [Daily Cron Job](#daily-cron-job)
- [Folder Structure](#folder-structure)
- [Database Indexing Strategy](#database-indexing-strategy)

## Overview
This deduplication system works with a database of approximately 350,000 products. It identifies duplicate products based on the following criteria:
- **Product ID** (available in product tags)
- **Gender**
- **Product Category**

### Objectives
1. Generate daily product merge suggestions and store them in `product_duplicates` and `product_duplicate_lists` tables.
2. Retrieve merge suggestions using optimized queries.

## Features
- Automated duplicate detection based on defined criteria.
- Organized insertion of duplicate group information into the database.
- Daily cron job for continuous updates and management.
- Logging for monitoring and debugging.
- For cron and also timestamp on program adapt to UTC+7/WIB Timezone.

## Requirements
- Python 3.8+
- MySQL
- Libraries:
  - SQLAlchemy
  - Pandas

## Setup Instructions
### 1. Clone the Repository
```bash
$ git clone https://github.com/saindawn/product-merge-suggestion.git
$ cd product-deduplication-system
```

### 2. Install Dependencies
Use pip to install required Python libraries:
```bash
$ pip install -r requirements.txt
```

### 3. Configure Database
Set up a MySQL database with the provided schema. Update the `connection_string` in the scripts to reflect your database credentials.

### 4. Initialize the Database
Run the `Initiator_ProductMergeSuggestions.py` script to prepare the database for deduplication operations:
```bash
$ sudo /path/to/python /path/to/Initiator_ProductMergeSuggestions.py
```

## Usage
### Initial Setup
Before scheduling daily jobs, ensure the database is initialized using the `Initiator_ProductMergeSuggestions.py` script.

### Daily Cron Job
Example cron job configuration:
```bash
5 17 * * * sudo /path/to/python /path/to/Daily_ProductMergeSuggestions.py
```

#### Run Manually
```bash
$ python Daily_ProductMergeSuggestions.py
```

## Folder Structure
```
product-deduplication-system/
├── Daily_ProductMergeSuggestions.py      # Script for daily deduplication jobs
├── Initiator_ProductMergeSuggestions.py  # Script for initial database setup
├── README.md                     # Documentation
├── get_merged_suggestions.sql    # SQL query to retrieve the list of merge suggestions
├── merge_utils.py                # Core utilities for database handling and merge logic
├── requirements.txt              # List of Python dependencies
```
# Database Indexing Strategy

To optimize the performance of frequently queried fields and improve data retrieval efficiency, the following indexes should be added to the database:

## SQL Commands for Index Creation

```sql
-- Add index on product_duplicate_id in product_duplicate_details table
CREATE INDEX idx_product_duplicate_id ON product_duplicate_details (product_duplicate_id);

-- Add index on updated_at in product_duplicate table
CREATE INDEX idx_updated_at ON product_duplicate (updated_at);
