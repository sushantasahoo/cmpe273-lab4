package edu.sjsu.cmpe.cache.client;

public class Client {

	public static void main(String[] args) throws Exception {

		System.out.println("Writing to all servers: key: 1, value: a ");
		CRDTClient crdtClient = new CRDTClient();
		boolean requestStatus = crdtClient.put(1, "a");

		if (requestStatus) {
			System.out
					.println("Sleeping for 15 seconds, turn off some servers to test repair functionality.");
			Thread.sleep(15000);
			System.out.println("Writing to all servers: key: 1, value: b ");
			requestStatus = crdtClient.put(1, "b");
			if (requestStatus) {
				System.out
						.println("Sleeping for 15 seconds, turn on dead servers.");
				Thread.sleep(15000);
				String value = crdtClient.get(1);
				System.out.println("Value retrieved for key 1: " + value);
			} else {
				System.out.println("Writing failed for key:1 , value: b");
			}
		} else {
			System.out.println("Writing failed for key:1 , value: a");
		}

	}

}
