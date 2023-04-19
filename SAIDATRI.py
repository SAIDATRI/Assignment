#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install mysql-connector-python


# In[2]:


import mysql.connector
from mysql.connector import Error
import pandas as pd


# In[3]:


def create_server_connection(host_name,user_name,user_password):    
    global connection
    connection = None    
    try:
        connection = mysql.connector.connect(
        host = host_name,
        user = user_name,
            password = user_password
        )
        print("MySQL connection Successfull")        
    except Error as er:
        print(f"Error: {er}")        
    return connection
ps = "jeevansh1802"
db = "Internship"
create_server_connection("localhost","root",ps)


# In[4]:


def create_database(connection,query):
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        print("Database Created Successfully")
        
    except Error as er:
        print(f"Error: {er}")
        
create_database_query = "Create Database Internship"
create_database(connection,create_database_query)
# create_database(connection,"Create Database Internship")


# In[5]:


def create_db_connection(host_name,user_name,user_password,db_name):
    connection = None
    
    try:
        connection = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = user_password,
        database = db_name
        )
        print("MYSQL Database Connection Successful")
        
    except Error as er:
        print(f"Error: {er}")
        
    return connection


# In[6]:


global cursor

def execute_query(connection,query):
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        connection.commit()
        print("Query was Successful")
        
    except Error as er:
        print(f"Error: {er}")


# In[7]:


create_orders_table = """
Create table orders(
order_id int primary key,
customer_name varchar(30) not null,
product_name varchar(30) not null,
date_ordered date,
quantity int,
unit_price float,
phone_number varchar(30));
"""

connection = create_db_connection("localhost","root",ps,db)
execute_query(connection,create_orders_table)


# In[8]:


data_orders = """
insert into orders values
(101,'Steve',"laptop","2008-06-12",2,800,"9654875412"),
(102, 'Jos', 'Books', '2019-02-10', 10, 12, '8367489124'),
(103, 'Stacy', 'Trousers', '2019-12-25', 5, 50, '8976123645'),
(104, 'Nancy', 'T-Shirts', '2018-07-14', 7, 30, '7368145099'),
(105, 'Maria', 'Headphones', '2019-05-30', 6, 48, '8865316698'), 
(106, 'Danny', 'Smart TV', '2018-08-20', 10, 300, '7720130449');
"""

connection = create_db_connection("localhost","root",ps,db)
execute_query(connection,data_orders)


# In[9]:


def read_query(connection,query):
    cursor = connection.cursor()
    results = None
    
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as er:
        print(f"Error: {er}")


# In[10]:


Q1 = """
Select * from orders;
"""

connection = create_db_connection("localhost","root",ps,db)
results = read_query(connection,Q1)

for result in results:
    print(result)


# In[11]:


from_db = []

for result in results:
    result = list(result)
    from_db.append(result)
    
columns = ["order_id", "customer_name", "product_name", "date_ordered", "quantity", "unit_price", "phone_number"]

df = pd.DataFrame(from_db,columns = columns)
display(df)


# In[12]:


Q2 = """
SELECT DISTINCT year(date_ordered) FROM orders;
"""

connection = create_db_connection("localhost","root",ps,db)
results = read_query(connection,Q2)

for result in results:
    print(result)


# In[14]:


Q11 = """
DELETE FROM orders WHERE unit_price='12';
"""

connection = create_db_connection("localhost","root",ps,db)
execute_query(connection,Q11)


# In[17]:


Q12 = """
UPDATE orders
SET order_id=100, customer_name='vinod', product_name='smart phone', date_ordered='2023-02-18', quantity=3,unit_price=40000, phone_number='9948167381'
WHERE order_id=101;
"""

connection = create_db_connection("localhost","root",ps,db)
execute_query(connection,Q12)


# In[19]:


Q13 = """
SELECT *
FROM orders 
WHERE unit_price>=50;
"""

connection = create_db_connection("localhost","root",ps,db)
results = read_query(connection,Q13)

from_db = []

for result in results:
    result = list(result)
    from_db.append(result)
    
columns = ["order_id", "customer_name", "product_name", "date_ordered", "quantity", "unit_price", "phone_number"]

df = pd.DataFrame(from_db,columns = columns)
display(df)


# In[20]:


Q14 = """
SELECT order_id, customer_name, product_name, date_ordered, quantity, unit_price, phone_number
FROM orders 
ORDER BY quantity ASC; 
"""

connection = create_db_connection("localhost","root",ps,db)
results = read_query(connection,Q14)

for result in results:
    print(result)


# In[21]:


Q15 = """
Select * from orders;
"""

connection = create_db_connection("localhost","root",ps,db)
results = read_query(connection,Q15)

for result in results:
    print(result)


# In[ ]:




