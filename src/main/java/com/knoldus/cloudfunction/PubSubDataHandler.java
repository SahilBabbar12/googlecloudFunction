package com.knoldus.cloudfunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.events.cloud.pubsub.v1.Message;
import com.knoldus.cloudfunction.model.Vehicle;
import io.cloudevents.CloudEvent;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Handles CloudEvents containing Pub/Sub data.
 */
public class PubSubDataHandler implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(
          PubSubDataHandler.class.getName());

  private String currencyApiKey;
  private String currencyApiUrl;

  private static final double MILEAGE_CONVERSION_RATE_ = 1.609344;
  /**
   * The Firestore instance for
   * interacting with the Firestore database.
   */
  private static Firestore firestore;

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

  private Properties loadConfigProperties() {
    Properties properties = new Properties();
    try (InputStream input = getClass().getResourceAsStream("/config.properties")) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return properties;
  }
  /**
   * Processes the incoming CloudEvent containing Pub/Sub data.
   *
   * @param event The incoming CloudEvent.
   * @throws JsonProcessingException
   * If there is an error parsing the JSON data.
   */
  @Override
  public void accept(final CloudEvent event) throws IOException {
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

    Double conversionRate = fetchConversionRateFromAPI("INR");

    double priceInRupees = transformPrice(vehicleData.getPrice(), conversionRate);
    double mileageInKmpl = transformMileage(vehicleData
            .getMileage());

    vehicleData.setPrice(priceInRupees);
    vehicleData.setMileage(mileageInKmpl);

    logger.info("Mileage in kmpl: " + vehicleData
            .getMileage());
    logger.info("Price in rupees: " + vehicleData
            .getPrice());
    saveDataToFirestore(vehicleData);
  }

  private double fetchConversionRateFromAPI(String toCurrency) {
    Properties properties = loadConfigProperties();
    currencyApiKey = properties.getProperty("currency.api.key");
    currencyApiUrl = properties.getProperty("currency.api.url");
    try {
      String apiUrl = currencyApiUrl + currencyApiKey;
      URL url = new URL(apiUrl);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");

      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
         BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
          String inputLine;
          StringBuilder response = new StringBuilder();

          while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
          }

          String jsonResponse = response.toString();
          JSONObject json = new JSONObject(jsonResponse);
          JSONObject data = json.getJSONObject("data");

          if (data.has(toCurrency)) {
            JSONObject currencyEntry = data.getJSONObject(toCurrency);
            Double conversionRate = currencyEntry.getDouble("value");
            return conversionRate;
          } else {
            logger.info("Currency not found: " + toCurrency);
            return 0.0;
          }
      } else {
        logger.info("HTTP Request failed with response code: " + responseCode);
      }
    } catch (IOException ioException) {
      logger.info("Malformed URL: " + ioException.getMessage());
    }
    return 0.0;
  }

    /**
   * Converts the price from dollars to rupees.
   *
   * @param priceInDollars The price in dollars.
   * @return The price in rupees.
   */
  private double transformPrice(final double priceInDollars, final double conversionRate) {
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
  private void saveDataToFirestore(Vehicle vehicleData) {
    DocumentReference docRef = firestore.collection("Car").document();
    ApiFuture<WriteResult> result = docRef.set(vehicleData);
    try {
      result.get();
      logger.info("Message data saved to Firestore: " + vehicleData);
    } catch (InterruptedException | ExecutionException e) {
      logger.severe("Error saving message data to Firestore: " + e.getMessage());
    }
  }
}
