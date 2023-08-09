package com.knoldus.cloudfunction;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import io.cloudevents.CloudEvent;
import model.Vehicle;

import java.util.Base64;
import java.util.logging.Logger;

public class PubSubDataHandler implements CloudEventsFunction {
    private static final Logger logger = Logger.getLogger(PubSubDataHandler.class.getName());

    private static final double PRICE_CONVERSION_RATE_ = 82.11;
    private static final double MILEAGE_CONVERSION_RATE_ = 1.609344;

    private static Firestore firestore;
    private static Integer count = 0;

    public PubSubDataHandler() {
        try {
            firestore = FirestoreOptions.getDefaultInstance().getService();
        } catch (ApiException e) {
            logger.severe("Firestore initialization error: " + e.getMessage());
        }
    }

    @Override
    public void accept(final CloudEvent event) {
        try {
            String cloudEventData = new String(event.getData().toBytes());
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            MessagePublishedData data = objectMapper.readValue(cloudEventData, MessagePublishedData.class);
            Message message = data.getMessage();
            String encodedData = message.getData();
            String decodedData = new String(Base64.getDecoder().decode(encodedData));

            logger.info("Pub/Sub message: " + decodedData);

            Vehicle vehicleData = objectMapper.readValue(decodedData, Vehicle.class);
            logger.info("Received vehicle data: " + vehicleData.toString());

            double priceInRupees = transformPrice(vehicleData.getPrice());
            double mileageInKmpl = transformMileage(vehicleData.getMileage());

            vehicleData.setPrice(priceInRupees);
            vehicleData.setMileage(mileageInKmpl);

            logger.info("Mileage in kmpl: " + vehicleData.getMileage());
            logger.info("Price in rupees: " + vehicleData.getPrice());

            saveDataToFirestore(vehicleData);
            count++;

            logger.info("Event Counter: " + count);
        } catch (Exception e) {
            logger.info("Error processing CloudEvent: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private double transformPrice(final double priceInDollars) {
        double conversionRate = PRICE_CONVERSION_RATE_;
        return priceInDollars * conversionRate;
    }

    private double transformMileage(final double mileageInMiles) {
        double conversionFactor = MILEAGE_CONVERSION_RATE_;
        return mileageInMiles * conversionFactor;
    }

    void saveDataToFirestore(final Vehicle vehicleData) {
        try {
            DocumentReference destinationDocRef = firestore.collection("Car").document();
            destinationDocRef.set(vehicleData);
            logger.info("Saved data to Firestore. Count: " + count);
            count++;
        } catch (Exception e) {
            logger.info("Error saving data to Firestore: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
