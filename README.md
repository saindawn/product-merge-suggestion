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
- [Contributing](#contributing)
- [License](#license)

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

## Requirements
- Python 3.8+
- MySQL
- Libraries:
  - SQLAlchemy
  - PyMySQL
  - Pandas

## Setup Instructions
### 1. Clone the Repository
```bash
$ git clone https://github.com/yourusername/product-deduplication-system.git
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
$ python Initiator_ProductMergeSuggestions.py
```

## Usage
### Initial Setup
Before scheduling daily jobs, ensure the database is initialized using the `Initiator_ProductMergeSuggestions.py` script.

### Daily Cron Job
Schedule the `Daily_ProductMergeSuggestions.py` script to run daily for consistent deduplication updates.

#### Run Manually
```bash
$ python Daily_ProductMergeSuggestions.py
```

## Folder Structure
```
product-deduplication-system/
├── merge_utils.py               # Core utilities for database handling and merge logic
├── Initiator_ProductMergeSuggestions.py  # Script for initial database setup
├── Daily_ProductMergeSuggestions.py      # Script for daily deduplication jobs
├── requirements.txt            # List of Python dependencies
├── README.md                   # Documentation
```

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for feedback and discussions.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
