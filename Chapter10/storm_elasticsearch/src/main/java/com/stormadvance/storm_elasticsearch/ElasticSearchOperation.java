package com.stormadvance.storm_elasticsearch;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchOperation {

	private TransportClient client;

	public ElasticSearchOperation(List<String> esNodes) throws Exception {
		try {
			Settings settings = Settings.settingsBuilder()
					.put("cluster.name", "elasticsearch").build();
			client = TransportClient.builder().settings(settings).build();
			for (String esNode : esNodes) {
				client.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(esNode), 9300));
			}

		} catch (Exception e) {
			throw e;
		}

	}

	public void insert(Map<String, Object> data, String indexName, String indexMapping, String indexId) {
		client.prepareIndex(indexName, indexMapping, indexId)
				.setSource(data).get();
	}
	
	
	public static void main(String[] s){
		try{
			List<String> esNodes = new ArrayList<String>();
			esNodes.add("127.0.0.1");
			ElasticSearchOperation elasticSearchOperation  = new ElasticSearchOperation(esNodes);
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("name", "name");
			data.put("add", "add");
			elasticSearchOperation.insert(data,"indexName","indexMapping",UUID.randomUUID().toString());
		}catch(Exception e) {
			e.printStackTrace();
			//System.out.println(e);
		}
	}
	
}
