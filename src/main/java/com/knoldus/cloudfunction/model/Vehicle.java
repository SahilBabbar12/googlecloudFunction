package com.knoldus.cloudfunction.model;

import lombok.Data;

/**
 * Represents a Vehicle.
 */
@Data
public class Vehicle {
    /**
     * The unique identifier of the car.
     */
    private Integer carId;

    /**
     * The model of the car.
     */
    private String model;

    /**
     * The brand of the car.
     */
    private String brand;

    /**
     * The manufacturing year of the car.
     */
    private Integer year;

    /**
     * The color of the car.
     */
    private String color;

    /**
     * The mileage of the car.
     */
    private Double mileage;

    /**
     * The price of the car.
     */
    private Double price;

     /**
     * The quantity of the car in stock.
     */
    private Integer quantity;

    /**
     * The tax on the car.
     */
    private Double tax;

}
