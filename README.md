# recruitment-website
Building Data Pipeline for Recruitment Website Project.\\
Summary: Collect, process and store data for the recruitment website system. Building data flow to ETL data from the server
(Data lake) into Data Warehouse for analysis.\\

Step 1: Config env, test connecting and import logs data into data lake (Cassandra DB).\\
![image](https://github.com/hiepphde/recruitment-website/assets/84515603/3a32536b-7304-4c43-9b94-7d8e1e2986be)

Step 2: Use notebooks to make it easier to build data Pipeline.\\
  2.1: Initialize the environment and declare connection variables to Data Lake.\\
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/4dabc4da-0ae1-4ede-8ae7-3607a8112d36)\\
  2.2: Write down the data processing function, here the 'Create_time' column transforms to get the uuid of each event.\\
  ![image](https://github.com/hiepphde/recruitment-website/assets/84515603/1cf7da53-644d-4a78-932b-af2e67a92b37)

