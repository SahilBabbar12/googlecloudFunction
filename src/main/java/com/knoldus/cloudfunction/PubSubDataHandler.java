package com.knoldus.cloudfunction;

import com.fasterxml.jackson.core.JsonProcessingException;
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

/**
 * Handles CloudEvents containing Pub/Sub data.
 */
public class PubSubDataHandler implements CloudEventsFunction {
    private static final Logger logger = Logger.getLogger(
            PubSubDataHandler.class.getName());

    private static final double PRICE_CONVERSION_RATE_ = 82.11;
    private static final double MILEAGE_CONVERSION_RATE_ = 1.609344;

    /**
     * The Firestore instance for
     * interacting with the Firestore database.
     */
    private static Firestore firestore;
    private  Integer count =0;

    /**
     * Constructor for the PubSubDataHandler class.
     * Initializes the Firestore instance.
     */
    public PubSubDataHandler() {
        try {
            firestore = FirestoreOptions
                    .getDefaultInstance().getService();
        } catch (ApiException e) {
            logger.severe("Firestore initialization error: "
                    + e.getMessage());
        }
    }
    /**
     * Processes the incoming CloudEvent containing Pub/Sub data.
     *
     * @param event The incoming CloudEvent.
     * @throws JsonProcessingException
     * If there is an error parsing the JSON data.
     */
    @Override
    public void accept(final CloudEvent event) throws JsonProcessingException {
        String cloudEventData = new String(event.getData().toBytes());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature
                .FAIL_ON_UNKNOWN_PROPERTIES, false);
        MessagePublishedData data = objectMapper.readValue(cloudEventData, MessagePublishedData.class);
        Message message = data.getMessage();
        String encodedData = message.getData();
        String decodedData = new String(Base64.getDecoder().decode(encodedData));

        logger.info("Pub/Sub message: " + decodedData);

        Vehicle vehicleData = objectMapper.readValue(decodedData, Vehicle.class);
        logger.info(vehicleData.toString());

        double priceInRupees = transformPrice(vehicleData
                .getPrice());
        double mileageInKmpl = transformMileage(vehicleData
                .getMileage());

        vehicleData.setPrice(priceInRupees);
        vehicleData.setMileage(mileageInKmpl);

        logger.info("Mileage in kmpl: " + vehicleData
                .getMileage());
        logger.info("Price in rupees: " + vehicleData
                .getPrice());
        saveDataToFirestore(vehicleData);
        count++;

        logger.info("Event Counter: " + count);
    }
    /**
     * Converts the price from dollars to rupees.
     *
     * @param priceInDollars The price in dollars.
     * @return The price in rupees.
     */
    private double transformPrice(final double priceInDollars) {
        double conversionRate = PRICE_CONVERSION_RATE_;
        return priceInDollars * conversionRate;
    }
    /**
     * Converts the mileage from miles to kilometers per liter.
     *
     * @param mileageInMiles The mileage in miles.
     * @return The mileage in kilometers per liter.
     */
    private double transformMileage(final double mileageInMiles) {
        double conversionFactor = MILEAGE_CONVERSION_RATE_;
        return mileageInMiles * conversionFactor;
    }
    /**
     * Saves the data from the provided
     * model.Vehicle object to Firestore.
     *
     * @param vehicleData
     * The model.Vehicle object containing the data to be saved.
     */
    void saveDataToFirestore(
            final Vehicle vehicleData) {
        DocumentReference destinationDocRef =
                firestore.collection("Car")
                        .document();
        destinationDocRef.set(vehicleData);
    }
}