package com.stormadvance.logprocessing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class takes referrer URL as input, analyze the URL and return search
 * keyword as output.
 * 
 */
public class KeywordGenerator {
	public String getKeyword(String referer) {

		String[] temp;
		Pattern pat = Pattern.compile("[?&#]q=([^&]+)");
		Matcher m = pat.matcher(referer);
		if (m.find()) {
			String searchTerm = null;
			searchTerm = m.group(1);
			temp = searchTerm.split("\\+");
			searchTerm = temp[0];
			for (int i = 1; i < temp.length; i++) {
				searchTerm = searchTerm + " " + temp[i];
			}
			return searchTerm;
		} else {
			pat = Pattern.compile("[?&#]p=([^&]+)");
			m = pat.matcher(referer);
			if (m.find()) {
				String searchTerm = null;
				searchTerm = m.group(1);
				temp = searchTerm.split("\\+");
				searchTerm = temp[0];
				for (int i = 1; i < temp.length; i++) {
					searchTerm = searchTerm + " " + temp[i];
				}
				return searchTerm;
			} else {
				//
				pat = Pattern.compile("[?&#]query=([^&]+)");
				m = pat.matcher(referer);
				if (m.find()) {
					String searchTerm = null;
					searchTerm = m.group(1);
					temp = searchTerm.split("\\+");
					searchTerm = temp[0];
					for (int i = 1; i < temp.length; i++) {
						searchTerm = searchTerm + " " + temp[i];
					}
					return searchTerm;
				}  else {
						return "NA";
					}
				}
		}
	}
	
	public static void main(String[] abc) {
		System.out.println(new KeywordGenerator().getKeyword("https://in.search.yahoo.com/search;_ylt=AqH0NZe1hgPCzVap0PdKk7GuitIF?p=india+live+score&toggle=1&cop=mss&ei=UTF-8&fr=yfp-t-704"));
	}
	
}
