Azure Data Engineer Project- Adventure Works (GitHub files)

Architecture diagram: - 

![image](https://github.com/user-attachments/assets/75a9df75-6dba-4f31-8d50-201d8c168252)


Phase 1: - Dynamic ADF Pipeline 

![image](https://github.com/user-attachments/assets/2df2696b-5553-4289-824b-d8907d77c791)


Goal: Dynamically load CSV files from GitHub into the Bronze container in ADLS Gen2 using Azure Data Factory (ADF).
Steps: - 
•	In GitHub we have 10 csv files we need to dynamically copy all the CSV files and load into bronze container in storage account using Azure data factory.
•	Create one JSON file which is required to all the file name along with URL which is required to dynamically fetch the data. (Please find below JSON code)
•	Create new container name is “Parameter” and save below JSON file into this container.

Phase 2: - Data Transformation 

Goal: - We need to connect data which is available in the bronze container in ADLS gen 2 in databricks and we need to do some transformations on this data and load into silver container in ADLS gen 2 using pyspark notebook.

![image](https://github.com/user-attachments/assets/725a9ae9-57f4-4e5c-9ded-6c6360444033)
