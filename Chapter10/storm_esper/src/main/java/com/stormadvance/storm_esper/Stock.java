package com.stormadvance.storm_esper;

import java.util.Date;

public class Stock {
	String product;
	Double price;
	Date timeStamp;

	public Stock(String s, double p, long t) {
		product = s;
		price = p;
		timeStamp = new Date(t);
	}

	public double getPrice() {
		return price;
	}

	public String getProduct() {
		return product;
	}

	public Date getTimeStamp() {
		return timeStamp;
	}

	@Override
	public String toString() {
		return "Price: " + price.toString() + " time: " + timeStamp.toString();
	}

}
