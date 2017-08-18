package com.stormadvance.storm_esper;

import java.util.Date;
import java.util.Random;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class EsperOperation {

	private EPRuntime cepRT = null;

	public EsperOperation() {
		Configuration cepConfig = new Configuration();
		cepConfig.addEventType("StockTick", Stock.class.getName());
		EPServiceProvider cep = EPServiceProviderManager.getProvider(
				"myCEPEngine", cepConfig);
		cepRT = cep.getEPRuntime();

		EPAdministrator cepAdm = cep.getEPAdministrator();
		EPStatement cepStatement = cepAdm
				.createEPL("select sum(price),product from "
						+ "StockTick.win:time_batch(5 sec) "
						+ "group by product");

		cepStatement.addListener(new CEPListener());
	}

	public static class CEPListener implements UpdateListener {

		public void update(EventBean[] newData, EventBean[] oldData) {
			try {
				System.out.println("#################### Event received: "+newData);
				for (EventBean eventBean : newData) {
					System.out.println("************************ Event received 1: "
							+ eventBean.getUnderlying());
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e);
			}
		}
	}

	public void esperPut(Stock stock) {
		cepRT.sendEvent(stock);
	}

	private static Random generator = new Random();

	public static void main(String[] s) throws InterruptedException {
		EsperOperation esperOperation = new EsperOperation();
		// We generate a few ticks...
		for (int i = 0; i < 5; i++) {
			double price = (double) generator.nextInt(10);
			long timeStamp = System.currentTimeMillis();
			String product = "AAPL";
			Stock stock = new Stock(product, price, timeStamp);
			System.out.println("Sending tick:" + stock);
			esperOperation.esperPut(stock);
		}
		Thread.sleep(200000);
	}

}
