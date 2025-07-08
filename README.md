****Azure Data Engineer Project- Adventure Works (GitHub files)****

**Architecture diagram: -** 

![image](https://github.com/user-attachments/assets/75a9df75-6dba-4f31-8d50-201d8c168252)


**Phase 1: - Dynamic ADF Pipeline**

![image](https://github.com/user-attachments/assets/2df2696b-5553-4289-824b-d8907d77c791)


Goal: Dynamically load CSV files from GitHub into the Bronze container in ADLS Gen2 using Azure Data Factory (ADF).
Steps: - 
•	In GitHub we have 10 csv files we need to dynamically copy all the CSV files and load into bronze container in storage account using Azure data factory.
•	Create one JSON file which is required to all the file name along with URL which is required to dynamically fetch the data. (Please find below JSON code)
•	Create new container name is “Parameter” and save below JSON file into this container.

**Phase 2: - Data Transformation** 

![image](https://github.com/user-attachments/assets/725a9ae9-57f4-4e5c-9ded-6c6360444033)

Goal: - We need to connect data which is available in the bronze container in ADLS gen 2 in databricks and we need to do some transformations on this data and load into silver container in ADLS gen 2 using pyspark notebook.

**Phase 3: - Datawarehouse**

![image](https://github.com/user-attachments/assets/3ccad1d4-b812-4e36-a845-bf80f951dfea)

Goal: - We can fetch the data from silver containers and load into server less SQL pool for connecting the data to Power BI for visualization.

**Phase 4: - Power BI (Synapse workspace connection)**

![image](https://github.com/user-attachments/assets/0adf049d-aef9-45b4-89fb-cf84bcd5b6c7)


•	You can copy serverless SQL endpoint connection from Azure synapse workspace and add into server textbox.
•	It needs connection you can pass database connection when we have created synapse analytics, we have provided user and password you can pass here otherwise you can access though Microsoft account. 
•	Once you provide a connection you can check the external tables and load the data into Power BI for generating the power Bi report.



![image](https://github.com/user-attachments/assets/efb3a226-41e1-4455-a51f-3443a99c225f)
