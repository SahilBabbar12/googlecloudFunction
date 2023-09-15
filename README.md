# Vehicle Data Processing with Cloud Functions and Firestore
This repository showcases an example implementation for processing and storing vehicle data using Google Cloud Functions and Firestore. The project highlights the seamless integration of event-driven architecture, where a cloud function is automatically triggered when new data is published to the "Vehicle" topic. The cloud function performs data transformations and securely stores the processed data into Firestore, a scalable NoSQL document database provided by Google Cloud Platform.
## Key Features
+ **Cloud Function**: The cloud function is designed to respond to data published on the "Vehicle" topic. It enables real-time data processing and transformation.

+ **Data Transformation**: The cloud function performs comprehensive data transformations on incoming vehicle data, allowing you to adapt the logic based on specific use cases.

+ **Firestore Integration**: The processed vehicle data is efficiently stored in Firestore, enabling seamless querying, retrieval, and analysis of vehicle information.
## Getting Started

To utilize this project, follow the steps below
### Google Cloud Platform (GCP) Project Setup:

+ Create a new project on Google Cloud Platform (GCP) if you haven't already.
+ Enable Firestore and Cloud Functions for the project.
+ Obtain the project ID.

### Service Account Credentials Setup:
+ Create a service account on GCP with the necessary permissions to access Firestore and Pub/Sub.
+ Download the service account credentials in JSON format and store them securely.
+ Set the path to the JSON file containing the credentials in the project's configuration.

### Pub/Sub Topic Setup:
+ Create a Pub/Sub topic and subscription.
+ Set the topic name in the project's configuration.

### Firestore Collection Setup:
+ Create a Firestore collection where the transformed data will be stored.
+ Set the collection name in the project's configuration.

### Deploying the Cloud Function:
+ Deploy the cloud function to Google Cloud Platform.
+ Open the console and navigate to the project directory.Run the following command to deploy the cloud function.
    
      gcloud alpha functions deploy cloud_function --entry-point pubsub.PubSubDataHandler --runtime java17 --trigger-topic topic_name

### Publishing a Message to the Topic
After deploying the cloud function, publish a message to the configured topic.
    The message should be in the format of the model class.
    Example message:

        {
            "carId": 123,
            "carModel": "Example Model",
            "brand": "Example Brand",
            "year": 2023,
            "color": "Red",
            "mileage": 5000.0,
            "price": 25000.0,
            "location": "London"
        }   

### Monitoring Execution and Error Logs
Check the logs to monitor the execution and any error messages on the cloud function.
+ Open the Firestore service and navigate to the configured collection to check the stored data.
