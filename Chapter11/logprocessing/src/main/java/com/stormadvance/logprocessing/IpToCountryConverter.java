package com.stormadvance.logprocessing;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

/**
 * This class contains logic to calculate the country name from IP address
 * 
 */
public class IpToCountryConverter {

	private static LookupService cl = null;

	/**
	 * An parameterised constructor which would take the location of
	 * GeoLiteCity.dat file as input.
	 * 
	 * @param pathTOGeoLiteCityFile
	 */
	public IpToCountryConverter(String pathTOGeoLiteCityFile) {
		try {
			cl = new LookupService("pathTOGeoLiteCityFile",
					LookupService.GEOIP_MEMORY_CACHE);
		} catch (Exception exception) {
			throw new RuntimeException(
					"Error occure while initializing IpToCountryConverter class : ");
		}
	}

	/**
	 * This method takes ip address an input and convert it into country name.
	 * 
	 * @param ip
	 * @return
	 */
	public String ipToCountry(String ip) {
		Location location = cl.getLocation(ip);
		if (location == null) {
			return "NA";
		}
		if (location.countryName == null) {
			return "NA";
		}
		return location.countryName;
	}
}