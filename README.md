# ETL Pipeline with Python, Pandas and PostgreSQL

This is a project that I made to teach myself the concept of ETL Pipeline, based on [this page](https://medium.com/analytics-vidhya/design-an-etl-pipeline-using-pandas-fd42adcb21f7) by Vivek Chaudhary.

ETL stands for **E**xtract, **T**ransform and **L**oad which is a set of processes to extract the data from one or more input sources, transform or clean the data so that it will be in the appropriate format and finally loading the data into an output destination such as a database, data mart, or a data warehouse.

In this project, the data is exctracted from a database (PostgreSQL), cleaned using Pandas methods and loaded to a database.

## Deploying

* Download and run the PostgreSQL installer: https://www.postgresql.org/, or get it through the package manager of your distribution if you're using Linux.

* Leave the default port 5432, and other default values during the installation.

* You will be asked to provide a password for the superuser (postgres), remember this password because it will be used later.

* Make sure to have Python installed ().

* Install the packages necessary via the command: `pip install sqlalchemy pandas psycopg2`.

* Python 3.9 and Numpy 1.19.4 has trouble running this code, so use Python 3.8 and Numpy 1.19.3: `pip install numpy==1.19.3`

* Run the `main.py` script.