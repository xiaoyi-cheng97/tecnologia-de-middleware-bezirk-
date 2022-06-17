package Client;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import org.apache.zookeeper.KeeperException;

import Zookeeper.ZKManager;

public class VerifyClient extends TimerTask {

	private Client client;
	private ZKManager zoo;

	public VerifyClient(Client client, ZKManager zoo) {
		this.client = client;
		this.zoo = zoo;
	}

	@Override
	public void run() {
		try {
			List<String> sourceNodeNames = client.getPath();
			List<String> allClientData = new ArrayList<>();
			
			for (String sourceName : sourceNodeNames) {
				List<String> children = zoo.listGroupChildren(sourceName.substring(1));
				List<String> clientData = new ArrayList<>();
				
				for (String child : children) {
					clientData.add((String) zoo.getZNodeData(sourceName.substring(1), child, null));
				}
				
				allClientData.addAll(clientData);
			}
			
			client.updateClientData(allClientData);
			client.resetTimer();
			
		} catch (InterruptedException | KeeperException e) {
			client.resetTimer();
		}
	}
}
