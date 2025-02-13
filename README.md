# Jupyter notebook over Apache Iceberg and AWS Glue

## Intro

This project demonstrates how to set up and use Jupyter notebooks with Apache Iceberg and AWS Glue, providing a practical guide for interactive data analysis and exploration on AWS. It covers configuration, data loading, querying, and basic data manipulation using PySpark within a Jupyter environment.

The target audience are Data Engineers or any person who needs to take initial contact with Iceberg and AWS Glue Catalog.

Just a sample project showing samples regading Iceberg table format interactions for Building AWS Glue jobs at Jupyter Notebooks using PySpark, Iceberg table, and AWS Glue Catalog.


Initial Considerations:

- Use of the AWS Glue Docker image, which provides a pre-configured environment with Spark and necessary libraries.
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image

- AWS Credentials: Mount your ~/.aws directory, which is convenient but not recommended for production. AM roles are much more secure.

- Workspace mounting at my current directory as my jupyter workspace.

- Port Mappingsthe necessary ports for JupyterLab and potentially Spark UIs.

- Create a Database and Hive-compatible Iceberg table.

- Structured S3 directories in the way warehouse/database/table/[data|metadata] for better organization






## Requirements
An active AWS account with permissions to create and manage resources in:
    *   AWS Glue (Data Catalog, Crawler, Jobs)
    *   Amazon S3 (Buckets, Objects)
    *   IAM (Roles, Policies)
*   [AWS CLI](https://aws.amazon.com/cli/) installed and configured with appropriate credentials..
*   Python 3.11 or later.
*   Jupyter Notebook or JupyterLab.
*   PySpark (version compatible with your AWS Glue environment).
*   Java (version compatible with your Spark and Iceberg environment).


## Installation / Setup Instructions
### Clone repository
```bash
git clone https://github.com/andermaco/Jupyter_notebook_over_Apache_Iceberg_and_AWS_Glue.git
cd Jupyter_notebook_over_Apache_Iceberg_and_AWS_Glue
```
### Configure environment
```bash
conda create -n jupyter_iceberg_glue python=3.8
conda activate jupyter_iceberg_glue
pip install -r requirements.txt
```

<br/>

### Installation and Setup

1.  **Clone the repository:**

    ```bash
    git clone [https://github.com/andermaco/Jupyter_notebook_over_Apache_Iceberg_and_AWS_Glue.git](https://github.com/andermaco/Jupyter_notebook_over_Apache_Iceberg_and_AWS_Glue.git)
    cd Jupyter_notebook_over_Apache_Iceberg_and_AWS_Glue
    ```

2.  **Create and activate a virtual environment (recommended):**

    ```bash
    conda create -n jupyter_iceberg_glue python=3.8
    conda activate jupyter_iceberg_glue
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

<br/>

## Configuration setup

1.  **To setup AWS credentials:** 

    Create a AWS profile at ~/.aws/credentials with the needed AWS credentials
    ```
    [user_name]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY
    ```

2.  **To set up AWS ROLE to be used:**

    Create the AWS ROLE trusted policy enabling AWS_USER to sts:AssumeRole
    ```sh
    echo """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com",
                    "AWS": "arn:aws:iam::<YOUR_AWS_ACCOUNT>:user/<AWS_USER_ARN>"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    """ > ~/role_policy.json
    ```

3. **Add following policies to the role for sample porpouse:**

    ```
    AmazonS3FullAccess
    AWSGlueServiceRole
    CloudWatchLogsFullAccess
    ``` 

4.  **User defined must enable to sts:AssumeRole the AWS_ROLE:**

    ```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": "arn:aws:iam::<YOUR_AWS_ACCOUNT>:role/<AWS_ROLE_ARN>"
            }
        ]
    }
    ```

4.  **Download required libraries:**

    Requerired libraries has not been uploaded to repository, dowload them using:
    ```sh
    bash sh/download_libs.sh
    ```
    Jar libraries will be downloaded to ./deplo/jar_libraries


5. **Pull AWS Glue Docker image:**

    Docker pull a pre-configured environment with Spark and necessary libraries. Nevertheless, addtional libraries at deploy/jar_libraries will be added later on.
    ```sh
    docker pull amazon/aws-glue-libs:glue_libs_4.0.0_image_01
    ```

7. **Exec Docker container:**
    The docker image contains all needed for setting the Jupyter Notebook environment. Feel free to modify:
    - AWS_REGION: AWS Region
    - AWS_PROFILE: AWS user credentials 
    - DB_NAME: AWS Glue database name
    - TB_NAME: AWS Glue table name
    - BUCKET_NAME: S3 bucket where datawarehouse will be stored in the following way **warehouse/database/table/[data|metadata]** for better organization

    ```sh
    docker run -it --rm -w /home/glue_user/workspace/jupyter_workspace -v ~/.aws:/home/glue_user/.aws -v `pwd`:/home/glue_user/workspace/jupyter_workspace/  -e AWS_PROFILE=bd_user_tq -e AWS_ROLE=arn:aws:iam::216989113396:role/role_glue_tq -e AWS_REGION=eu-west-1 -e DISABLE_SSL=true -e DB_NAME=database_name -e TB_NAME=table_name -e BUCKET_NAME=bd-datawarehouse -e WORKGROUP=bd_test_tq_wg -p 4050:4040 -p 18050:18080 -p 8950:8998 -p 8888:8888 --name glue_pyspark_dev amazon/aws-glue-libs:glue_libs_4.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh
    ```
    Jupyter Notebook will be created

8.  **Enter at Jupyter Notebook:**
    ```
    Enter at http://127.0.0.1:8888/lab
    ```

<br/>

### Jupyter Notebook configuration setup

#### Start a new "Terminal" session
At the botton left, click on "Terminal"

Once new Terminal is available exec the following in order to configure aditional libraries and settings:

```sh
cd jupyter_workspace/
bash configure_env.sh
```

***Here in after, at Jupyter Notebook, open [sample_spark_preprocesing.ipynb](sample_spark_preprocesing.ipynb) jupyter notebook, exece all cells, it's self descriptive.***

<br/>

## Considerations about Iceberg table partition
Here you have a brief resume about the main goals I have found over the net.
### Small files problem
The small file problem in Iceberg occurs when too many small data files are created within partitions over time, leading to inefficiencies in query performance, storage, and compute resource utilization.

Causes of the Small File Problem
- Frequent Small Writes: If a system frequently appends small batches of data (e.g., streaming jobs, micro-batches), Iceberg creates many small Parquet, ORC, or Avro files.
- Partitioning Strategy: If a table is over-partitioned (e.g., partitioning by created_at at the day level instead of the month level), each partition may contain only a few records, leading to excessive small files.
- Compaction Delay: Iceberg provides data file rewriting (compaction), but if compaction is not triggered frequently, small files accumulate over time.
- Merge-on-Read (MoR) Tables and Deletes: When using Merge-on-Read, Iceberg stores delete operations as delete files instead of rewriting affected data files immediately. This can increase the number of small files

#### Impact of Small Files in Iceberg
- Slower Query Performance: Reading thousands of small files instead of a few large ones increases metadata operations, scan time, and compute overhead.
- Higher Storage and Compute Costs: Storage engines (e.g., AWS S3, HDFS) charge based on the number of objects stored and retrieved. Too many small files increase storage costs and API request charges.
- High Metadata Overhead: Iceberg tracks files in metadata layers. Having many small files bloats metadata tables, increasing query planning time.
- Inefficient Resource Utilization: Query engines (e.g., Spark, Trino, Athena) allocate tasks based on file count. More small files mean higher task scheduling overhead and resource waste.




This is a common issue in append-heavy workloads, where frequent writes create new small files instead of consolidating data into larger, optimized files.
https://www.dremio.com/blog/compaction-in-apache-iceberg-fine-tuning-your-iceberg-tables-data-files/

### The importance of defining Iceberg table properties
When Iceberg table is created using DDL, take special care about table properties definition:
- **format**: Set to Iceberg. Iceberg table format could also configured not as table property but as table definition by 'USING ICEBERG'
    ```sql
    CREATE TABLE prod.db.sample (
        id bigint NOT NULL COMMENT 'unique id',
        data string)
    USING iceberg;
    ```
- **write_compression**: There are different options based on engine version and file format, check https://docs.aws.amazon.com/athena/latest/ug/compression-support-iceberg.html
- **optimize_rewrite_data_file_threshold**: Controls when Iceberg should rewrite data files during an **OPTIMIZE** operation. A lower value means than if there are fewer data files that require optimization than the given threshold, the files are not rewritten. 
- **optimize_rewrite_delete_file_threshold**: Determines the minimum number of delete files required in a partition before an automatic rewrite by **OPTIMIZE**
**vacuum_min_snapshots_to_keep**: Minimum number of snapshots to retain on a table's main branch.
- ... (Check documentation)

You can check those table parameters doing:
```sql
describe formatted  database_name.table_name;
```


### How Iceberg partition works
Remember, we have use hiddent partitions at Iceberg, that means **not to store partition columns explicitly in the table schema**, instead, it derives partition values from existing columns using partition transforms. In our case, month(created_at) extracts the year and month from the created_at timestamp and stores data under that partition.  
The data files are stored in S3 under year-month partitions based on the created_at column in your dataset, "**/created_at_month=YYYY-MM/**" path (not folder) in your storage (s3 in my case)
```sh
...
database_name/table_name/data/created_at_month=2024-12/20250207_060540_00025_7ux8w-fc9384e7-b9db-4b4a-87e0-794455250178.parquet 
database_name/table_name/data/created_at_month=2025-01/20250207_060448_00022_frrcj-9f6dd53c-a582-45ec-8197-5314772e81ac.parquet 
database_name/table_name/data/created_at_month=2025-02/20250206_193043_00194_brygj-e1502c45-27a5-4579-ab54-5bd3c6407984.parquet 
...
```
The partition column created_at_month is generated dynamically from the created_at column.
Each partition represents all data for a specific month.
The actual Parquet files are stored inside these partitions.

### How Iceberg Handles Queries on This Partitioned Table
When you run a query like:
```sql
SELECT * FROM database_name.table_name WHERE created_at >= '2025-02-01' AND created_at < '2025-03-01';
```
Iceberg prunes partitions and only reads the files under created_at_month=2025-02/ instead of scanning the entire dataset.
This improves performance significantly compared to traditional partitioning systems like Hive.

In example, run in Athena a sample query like below one, and check the 'Data Scanned' value.
```sql
SELECT *
FROM database_name.table_name
WHERE created_at >= date('2025-01-01') and created_at <= date('2025-02-02');
```

You can get more info about table properties here: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html


### Athena & Iceberg Time Travel and Snapshots capability
There are several posibilities to time travel over our data in Athena-Iceberg

##### Time Travel 
###### Query data at a specific point in time
Use the sentence **FOR TIMESTAMP AS OF** to query to an specific timestamp.
```sql
SELECT * FROM database_name.table_name FOR TIMESTAMP AS OF TIMESTAMP '2025-02-09 19:50:31.005000 UTC'
```

###### Restore a table to a specific point in time using CTAS
```sql
CREATE TABLE restored_table_name AS
SELECT *
FROM table_name
FOR TIMESTAMP AS OF TIMESTAMP '2025-02-09 19:50:31.005 UTC'
```
<br/>



##### Snapshots
###### List Snapshots for a Table
In order to view the available snapshots (committed_at and snapshot_id fields) for an Iceberg table, query the $snapshots metadata table
```sql
SELECT * FROM database_name."table_name$snapshots"
```

###### Query data at a Specific Snapshot ID
Use the sentence **FOR VERSION AS OF** to query to an specific snapshot id
```sql
SELECT * FROM database_name.table_name FOR VERSION AS OF 6132392547015257590
```

###### Create a view from a specific snapshot ID
```sql
CREATE OR REPLACE VIEW time_travel_view AS
SELECT *
FROM database_name.table_name
FOR VERSION AS OF 6132392547015257590;
```

<br/>
# License

Copyright [2025] [Ander Martínez]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
