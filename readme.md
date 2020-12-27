
# Norton Lifelock

Data Engineer - Take Home Challenge

### Objective

Write a Spark (2.4.5) application using Scala (2.11) with Maven as the automation tool.

### Overview

This application should read two files in jsonl format, join the data sets, perform some light transformations, aggregate the data, and save it to a spark sql table. A lot of our code has complex business logic that needs testing, so we prefer compiled Spark code for that reason. This exercise will give us some insights into how you approach the problem as well as your familiarity with the various technical requirements (see below). If you have questions about the requirements, don't hesistate to reach out to your interviewer.

### Technical Requirements

- All dependencies from Maven central
- Spark 2.4.5 using Scala 2.11
- Business logic should be unit tested where applicable
- Application should have an integration test (local spark sql temp tables are fine for this purpose, no need for outside dependencies)
- It should be resilient, what happens when a join fails because left or right side is missing? What happens when a value is null or out of range?
- Should be configurable, for example the destination table name and input file names may differ from a local development environment and a production environment
- Should log important information
- Should not encounter a NotSerializableException when run within a spark cluster
- Bonus points for JSON schema validation per json-schema.org

### Business Requirements                                        

- combine data sets from both files using student_id
- exclude all test results taken on 2020-04-05, apparently there was an administration error and those tests are inavlid!
- substitute school_id for real school name (see School ID Map below)
- aggregate on student_id and generate average test score as well as count of tests taken
- final schema should contain: student_id, student_name, avg_test_score, num_tests, address, school_name
- it should save that data in a spark sql table

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
### Solution: 

The above requirements shows these are the following areas needed for a programmer to code:
### Logging
### Exception Handling
### Code Structuring
### Config Management 
### Maven & Intellij 
### Unit testing using scalaTest
### Generate metrics

Here src->main->scala->org.norton.SampleAPP->SampleAPP is the main file that has all the main source code that does as per business requirements. 
The above file needs one argument --> it's environment parameter needed. (Eg: DEV or ITG or PROD)

src->main->scala->resources->student_info.json and test_scores.json are the json files given in above business requirements. 

src->main->scala->resources->finaldf_info.json is the final file created after runnning main src script

###Scala unit testing:

src->test->scala->org.norton.SampleAPP:

IsNullEmptyunction: It's a udf that is used in 2 unit test that check's if a column has null or empty values
SparkSessionTestWrapper: It's a trait that has spark session 
unitTesting: This is the main class that runs 4 unit tests:
1. Schema validater
2. Check school name mapped values
3. Check num of tests a student took (non null or empty values)
4. Check average test scores column (non null or empty values)






