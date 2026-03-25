# Data Engineering Zoomcamp 2026 Capstone Project

## WORK IN PROGRESS

TODO 1 - create a generic description and add important sections. The source and scope of the project are described here: <https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/projects>

TODO 2 - besides the TODO 1 with general description, all the important standard industry sections should be included with short and comprehensive information on how to: setup, build, run, deploy, etc

TODO 3 - check the terraform folder, check the main.tf and variables.tf files. Based on this, the user who will use current project, need to setup a Google Cloud Account, an account service (current project uses a single account service - for simplicity). The service account should have permissions to work with GCS - BigQuery Admin, Storage Admin. This part I believe belongs to the setup section (correct if it's not true).

TODO 4 - not important but nice to have (only if industry standard). Add a little section where you mention the /root/notebooks directory which is used for exploratory and dev coding

TODO 5 - read .env file. create example.env file, a file containing all env variable names without values, a file which is pushed to GIT. Also mention what to do with the example.env file in the setup section (the usual canonical way for a python project regarding .env file from sample.env file)

TODO 6 - under setup section add instruction to add a google cloud service account and to create an api key. the json file (api key) should be placed in the `dev` folder at the `project root`, e.g. `project-root/dev/gcp-capstone-project-keys.json`. The dev directory name and the json file name should match the exact string in the example. This is related to actions done on the google cloud and also related to TODO 3, for terraform to be able to perform it's job.

TODO 7 - this project uses GCP. In setup add a step which sets GOOGLE_APPLICATION_CREDENTIALS , which points to the account service api key json file (related to TODO 3).

TODO 8 - in setup section (control it), create a dev/ directory, create a json file named gcp-capstone-project-keys.json, paste the contents of the account service json key. This is needed for airflow running in docker (simplest way for capstone project).

TODO 9 - in airflow while running as a docker container, set an environment variable named GOOGLE_APPLICATION_CREDENTIALS with value  /opt/airflow/dev/gcp-capstone-project-keys.json (check Dockerfile.airflow). This way airflow will use it to connect to GCP. (related to TODO 6, may be combined or reworked).

TODO 10 - refactor ingestion. See dev/refactor-ingestion.txt (not for AI).

TODO 11 - setup for dbt. edit dbt/citibike/profile.yml with correct project id for GCP.

TODO 12 - extract project from profile.yml to a env var GCP_PROJECT_ID. Use it in airflow.

NOTE AI - IMPORTANT - START EDITING BENEATH THIS POINT. LET THE ABOVE TEXT BE AVAILABLE FOR REFERENCE