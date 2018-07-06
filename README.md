# Seal-IoT
Google Cloud IoT Sample for triggering and transforming telementry data from Sensors to BigQuery. Sensors would generate data like temperature, humidity, pressure, dewpoint, power consumption, latitude and longitude.

How to use:
-----------

Step 1. Clone this github repository in your cloudshell panel. git clone https://github.com/GCP-Cloud/Seal-IoT.git
Step 2. Create Cloud Storage bucket {Unique name of your choice}
Step 3. Create IoT Registry {Name of your choice}
Step 4. Create a Topic in PubSub {Name of your choice}
Step 5. cd into Seal-IoT and setup environment variables. Copy paste the below in your cloud shell. Do not leave space between '=' and projectName.
        export PROJECT=YourProjectName
        export REGION=us-central1
        export ZONE=us-central1-a
        export BUCKET=YourBucketName
        export REGISTRY=YourRegistryName
        export TOPIC=YourPubSubTopicName
Step 6. Important Step. Since your cloning from my repository, make sure you have set permissions for all files. Type ls -l and check if you (owner), group and others have RWX access. If not, please setup Read, write, execute (RWX) access for all. Since this is only a test environment, you should be fine. Don't use this for Production.
Use command chmod g+x filename -> to give execute access to group. Repeat the same for all files. If not, you will get permission denied error (5). 
Step 7. cd into bin as "cd bin"
Step 8. Create MQTT devices by executing ./create_cert.sh
Step 9. Registering your devices. Execute ./register.sh
Step 10. To create a pipeline, run ./job.sh from bin. Make sure you are in bin path
Step 11. To make devices you created generate messages, use ./run.sh
Step 12. Login to console and check bigquery for results. Give about a minute or two and refresh the bigquery page for results

Good luck!
