package Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import org.apache.zookeeper.KeeperException;

import Zookeeper.ZKManager;

public class VerifyTask extends TimerTask {
	private Source sc;
	private ZKManager zoo;
	
	public VerifyTask(Source source, ZKManager zoo) {
		this.sc = source;
		this.zoo = zoo;
	}

	@Override
	public void run() {
		try {
			
			String sourceNodeName = sc.SOURCE_NODE_PATHNAME+sc.getID();
			List<String> children = zoo.listGroupChildren(sourceNodeName);
			List<String> clientData = new ArrayList<>();
			
			for (String child : children) {
				clientData.add((String) zoo.getZNodeData(sourceNodeName, child, null));
			}
			sc.updateClientData(clientData);
			sc.resetTimer();
			
		} catch (InterruptedException | KeeperException e) {
			sc.resetTimer();
		}
		
	}
}
