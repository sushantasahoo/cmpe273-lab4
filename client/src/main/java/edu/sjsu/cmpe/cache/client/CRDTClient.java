package edu.sjsu.cmpe.cache.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class CRDTClient {
	List<CacheServiceInterface> serverList;
	// private int successfulWrites = 0;
	private final String SERVER_A = "http://localhost:3000";
	private final String SERVER_B = "http://localhost:3001";
	private final String SERVER_C = "http://localhost:3002";
	private final String NO_VAL = "null";

	private CountDownLatch countDownLatch;

	public CRDTClient() {
		serverList = new ArrayList<CacheServiceInterface>();
		serverList.add(new DistributedCacheService(SERVER_A));
		serverList.add(new DistributedCacheService(SERVER_B));
		serverList.add(new DistributedCacheService(SERVER_C));
	}

	public boolean put(long key, String value) throws InterruptedException,
			IOException {
		countDownLatch = new CountDownLatch(serverList.size());
		final AtomicInteger successfulWrites = new AtomicInteger(0);
		// final ArrayList<DistributedCacheService> writtenServerList = new
		// ArrayList<DistributedCacheService>(3);
		for (final CacheServiceInterface cacheServer : serverList) {
			// System.out.println("Putting to server "
			// + cacheServer.getCacheServerUrl());
			Future<HttpResponse<JsonNode>> future = Unirest
					.put(cacheServer.getCacheServerUrl()
							+ "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							countDownLatch.countDown();
							System.out
									.println("Write/Update failed for server "
											+ cacheServer.getCacheServerUrl());
							// e.printStackTrace();
						}

						public void completed(HttpResponse<JsonNode> response) {
							successfulWrites.incrementAndGet();
							countDownLatch.countDown();
							// writtenServerList.add(cacheServer);
							System.out.println("Write successful for server "
									+ cacheServer.getCacheServerUrl());
						}

						public void cancelled() {
							countDownLatch.countDown();
							System.out.println("Write cancelled for server "
									+ cacheServer.getCacheServerUrl());
						}

					});
		}

		countDownLatch.await();
		System.out.println("Write sucessful for number of servers "
				+ successfulWrites);
		if (successfulWrites.intValue() > 1) {
			return true;
		} else {
			this.countDownLatch = new CountDownLatch(serverList.size());

			System.out.println("Deleting partial write from servers");
			for (CacheServiceInterface cacheServer : serverList) {
				Future<HttpResponse<JsonNode>> future = Unirest
						.delete(cacheServer.getCacheServerUrl()
								+ "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							public void failed(UnirestException e) {
								countDownLatch.countDown();
								System.out.println("Delete failed..."
										+ cacheServer.getCacheServerUrl());
							}

							public void completed(
									HttpResponse<JsonNode> response) {
								countDownLatch.countDown();
								System.out.println("Delete is successful "
										+ cacheServer.getCacheServerUrl());
							}

							public void cancelled() {
								countDownLatch.countDown();
								System.out.println("Delete cancelled for "
										+ cacheServer.getCacheServerUrl());
							}
						});
			}
			Unirest.shutdown();
			return false;
		}
	}

	public String get(long key) throws InterruptedException, UnirestException,
			IOException {
		countDownLatch = new CountDownLatch(serverList.size());
		// asynchronously GET latest values
		final Map<String, Integer> resultCountMap = new HashMap<>();
		final Map<CacheServiceInterface, String> serverResultMap = new HashMap<>();
		for (CacheServiceInterface cacheServer : serverList) {
			// System.out.println("Getting value from "
			// + cacheServer.getCacheServerUrl());
			Future<HttpResponse<JsonNode>> future = Unirest
					.get(cacheServer.getCacheServerUrl() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							countDownLatch.countDown();
							serverResultMap.put(cacheServer, NO_VAL);
							System.out.println("Read failed for "
									+ cacheServer.getCacheServerUrl());
						}

						public void completed(HttpResponse<JsonNode> response) {
							System.out.println("Read successful for "
									+ cacheServer.getCacheServerUrl());

							/*
							 * if(response.getBody()==null){
							 * System.out.println("Body null"); }else
							 * if(response.getBody().getObject()==null){
							 * System.out.println("Object null"); }
							 */

							countDownLatch.countDown();
							String val = response.getBody() == null ? NO_VAL
									: response.getBody().getObject()
											.getString("value");
							// System.out.println("Read value: " + val);
							serverResultMap.put(cacheServer, val);

							if (resultCountMap.containsKey(val)) {
								resultCountMap.put(val,
										resultCountMap.get(val) + 1);
							} else {
								resultCountMap.put(val, 1);
							}

						}

						public void cancelled() {
							countDownLatch.countDown();
							serverResultMap.put(cacheServer, "");
							System.out.println("Read cancelled for "
									+ cacheServer.getCacheServerUrl());
						}
					});
		}
		countDownLatch.await();
		// retrieve value with max count
		String maxResponse = "";
		int maxResponseCount = 0;
		// try {
		for (String value : resultCountMap.keySet()) {
			if (resultCountMap.get(value) > maxResponseCount) {
				maxResponseCount = resultCountMap.get(value);
				maxResponse = value;

				System.out.println("maxCount " + resultCountMap.get(value));
				System.out.println("maxCount value " + value);

			}

		}
		/*
		 * } catch (Exception e) { System.out.println("caught null pointer"); }
		 */

		// read repair
		if (maxResponseCount != serverList.size()) {

			// Repair servers whose response is does not match with maxResponse
			for (CacheServiceInterface cacheServer : serverList) {
				if (serverResultMap.get(cacheServer) != null
						&& (!serverResultMap.get(cacheServer).equalsIgnoreCase(
								maxResponse))) {

					System.out.println("Repairing "
							+ cacheServer.getCacheServerUrl());
					HttpResponse<JsonNode> response = Unirest
							.put(cacheServer.getCacheServerUrl()
									+ "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", maxResponse).asJson();
				}
			}
		} else {
			System.out.println("Repair not required");
		}
		Unirest.shutdown();
		return maxResponse;
	}

	public String getKeyByValue(Map<String, Integer> map, int value) {
		for (Entry<String, Integer> entry : map.entrySet()) {
			if (value == entry.getValue())
				return entry.getKey();
		}
		return null;
	}
}