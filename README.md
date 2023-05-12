# recruitment-website
Building Data Pipeline for Recruitment Website Project.  
Summary: Collect, process and store data for the recruitment website system. Building data flow to ETL data from the server
(Data lake) into Data Warehouse for analysis.  

Step 1: Config env, test connecting and import logs data into data lake (Cassandra DB).  
![image](https://github.com/hiepphde/recruitment-website/assets/84515603/3a32536b-7304-4c43-9b94-7d8e1e2986be)

Step 2: Use notebooks to make it easier to build data Pipeline.  
  2.1: Initialize the Spark environment to perform large data processing.  
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/4dabc4da-0ae1-4ede-8ae7-3607a8112d36)  
  2.2: Write down the data processing function, here the 'Create_time' column transforms to get the uuid of each event.  
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/1cf7da53-644d-4a78-932b-af2e67a92b37)  
  2.3: Processing the Clicks value of each Events is created. Fill null values and create a temporary view table.  
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/9649089f-585b-4273-a6f5-47a53683473c)  
  Work similarly with conversion, Qualifield and Unqualified values.  
  2.4: Use the Join function to combine data to create Final DataFrame.  
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/358388b2-ba6f-4425-8c9b-9400ed36c881)  
  2.5: Because the 'Company_id' value is in another database, it is necessary to connect there to get data.  
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/1374d2ca-fc4b-483c-bca5-5c9ba30916d6)  
  => Coming here has finished processing data  
Step 3: Use MySQL to build Data Warehouse and connect to Data Warehouse to prepare for loading data  
![image](https://github.com/hiepphde/recruitment-website/assets/84515603/195613d3-09dc-40b1-b103-ba5041ffb0d9)  
  
In order to simulate for continuous data, I wrote a scripts Faking Data in Datalake, the Faking file was attached to the project.  
file 'faking-data-scripts.py'
  
Results after I run my application:  
![image](https://github.com/hiepphde/recruitment-website/assets/84515603/ed797641-fe0e-4e31-80db-8a21fd37a390)
  
This is a project that simulates the process of building a complete Data Pipeline with Near Real-time.  
I have learned how to operate Spark, get used to the NOSQL database and understand the process of building an ETL Scripts in practice.






