# Youtube-Data-Analysis-Project
This project focuses on securely handling, optimizing, and analyzing structured and semi-structured YouTube video data, categorized by video type and trending metrics.

# Project Goals
Data Ingestion: Develop a system to collect data from multiple sources efficiently.
saas
ETL Process: Convert raw data into a structured format through extraction, transformation, and loading.
Data Lake: Establish a centralized repository to store data from various sources for easy access and management.
Scalability: Ensure the system can handle increasing data volumes without performance issues.
Cloud Integration: Leverage AWS for large-scale data processing, eliminating local computing limitations.

# Dataset Used

This Kaggle dataset includes statistics (CSV files) on daily trending YouTube videos over several months. Each day, up to 200 popular videos from various regions are recorded, with separate files for each region. The dataset contains details such as video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count. Additionally, each region’s data includes a category_id field, which varies by location, along with a linked JSON file.
https://www.kaggle.com/datasets/datasnaek/youtube-new

# Services Used 
Amazon S3: A scalable object storage service that ensures high availability, security, and performance for storing structured and unstructured data.
AWS IAM: A security service that enables fine-grained access control and identity management for AWS resources.
AWS Glue: A serverless data integration service that simplifies data discovery, preparation, and transformation for analytics and machine learning.
AWS Lambda: A serverless computing service that runs code automatically in response to events, eliminating the need for server management.
AWS SNS: A fully managed messaging service that enables event-driven notifications and communication between distributed systems.
AWS EventBridge: A serverless event bus that helps connect different AWS services, enabling event-driven automation and real-time data processing.
AWS Athena: An interactive query service that allows users to run SQL queries on data stored in Amazon S3 without requiring data loading.
AWS Redshift: A fully managed cloud data warehouse that enables fast and scalable querying and analytics on large datasets.






