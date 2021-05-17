# COVID-19 New Delhi Dashboard
Developed an ETL pipeline to gather COVID-19 data and vaccination centres data in New Delhi daily.

**Extracted** daily data from the Delhi Chief Minister's Office's official Twitter account and Covid Vaccine Intelligence Network (CoWin) portal for vaccination data. 
The extracted image data were converted to text using Google Cloud Vision Text Detection API.

The data extracted was further **transformed** into usable data and format.

The data is **loaded** on AWS Redshift Data Warehouse.

Built an Apache Airflow DAG to trigger the ETL at 8 AM daily.

Created a dashboard using Grafana to represent the data :point_down:

![DASHBOARD SCREEN](/ScreenShot.jpg)
