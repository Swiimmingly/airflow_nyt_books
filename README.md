Official airflow docker-compose.yaml to setup the webserver, scheduler, postgres, redis, flower and environment. I modified airflow.cfg to setup the smtp.

Airflow dag gets the weekly updated bestseller fiction and Nonfiction e-books from [NYT API](https://developer.nytimes.com), and sends the lists to chosen email addresses.

Can be triggered manually by the admin to add aditional recipients through airflow dag configuration json.