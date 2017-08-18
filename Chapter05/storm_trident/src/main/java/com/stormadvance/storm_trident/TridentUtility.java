package com.stormadvance.storm_trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class TridentUtility {
	/**
	 * Get the comma separated value as input, split the field by comma, and
	 * then emits multiple tuple as output.
	 * 
	 */
	public static class Split extends BaseFunction {

		private static final long serialVersionUID = 2L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String countries = tuple.getString(0);
			for (String word : countries.split(",")) {
				// System.out.println("word -"+word);
				collector.emit(new Values(word));
			}
		}
	}

	/**
	 * This class extends BaseFilter and contain isKeep method which emits only
	 * those tuple which has #FIFA in text field.
	 */
	public static class TweetFilter extends BaseFilter {

		private static final long serialVersionUID = 1L;

		public boolean isKeep(TridentTuple tuple) {
			if (tuple.getString(0).contains("#FIFA")) {
				return true;
			} else {
				return false;
			}
		}

	}

	/**
	 * This class extends BaseFilter and contain isKeep method which will print
	 * the input tuple.
	 * 
	 */
	public static class Print extends BaseFilter {

		private static final long serialVersionUID = 1L;

		public boolean isKeep(TridentTuple tuple) {
			System.out.println(tuple);
			return true;
		}

	}
}
