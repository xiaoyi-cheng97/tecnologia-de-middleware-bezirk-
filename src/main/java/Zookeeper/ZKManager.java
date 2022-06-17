package Zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;


public class ZKManager {

	private static ZooKeeper zkeeper;
	private static ZKConnection zkConnection;

	public ZKManager() throws IOException, InterruptedException, KeeperException {
		initialize();
	}

	private void initialize() throws IOException, InterruptedException, KeeperException {
		zkConnection = new ZKConnection();
		zkeeper = zkConnection.connect("10.101.149.55");
		
	}

	public ZKManager(Watcher watcher) throws IOException, InterruptedException, KeeperException {
		initialize(watcher);
	}

	private void initialize(Watcher watcher) throws IOException, InterruptedException, KeeperException {
		zkConnection = new ZKConnection();
		zkeeper = zkConnection.connect("127.0.0.1", watcher);
	}

	public void closeConnection() throws InterruptedException {
		zkConnection.close();
	}

	public Object getZNodeData(String group, String member, Watcher watcher) throws KeeperException, InterruptedException {
		String path;
		if(member.compareTo("") == 0) {
			path = "/" + group;
		}else {
			path = "/" + group + "/" + member ;
		}
		
		if(znode_exists(path, true) == null) {
			return null;
		}
		
		try {
			byte[] b = null;
			b = zkeeper.getData(path, watcher, null);
			return new String(b, "UTF-8");
		} catch (InterruptedException | UnsupportedEncodingException e) {
			throw KeeperException.create(Code.APIERROR, path);
		}
	}

	public void update(String path, byte[] data, boolean watchFlag) throws KeeperException, InterruptedException {
		int version;
		if(znode_exists(path, watchFlag) == null) {
			return ;
		}
		try {
			version = zkeeper.exists(path, true).getVersion();
			zkeeper.setData(path, data, version);
		} catch (InterruptedException e) {
			throw KeeperException.create(Code.APIERROR, path);
		}
	}

	public String createGroup(String groupName, boolean watchFlag) throws KeeperException, InterruptedException {
		String path = "/" + groupName;

		Stat status = znode_exists(path, watchFlag);

		return status != null ? "" : zkeeper.create(path, null /* data */, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public String joinGroup(String groupName, String memberName, byte[] data, boolean watchFlag, boolean ephemeral, boolean isSource) throws KeeperException, InterruptedException {
		String path = "/" + groupName + "/" + memberName + "-";
		Stat status = znode_exists(path, watchFlag);		
		
		if (status == null) {		
			if (listGroupChildren(groupName).size() < 3 || isSource) {
				if(ephemeral)
					return zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				else
					return zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			else {
				StringBuilder sb = new StringBuilder("Full:");
				for (String b : listGroupChildren(groupName)) {
					sb.append(b + ",");
				
				}
				
				return sb.toString();
			}
		}
		
		
		return "Erro!";
		// in real world would probably be persistent but for the sake of simplicty here
		// its epheremeral
	}
	

	public void deleteGroup(String groupName, boolean watchFlag) throws KeeperException, InterruptedException {
		String path = "/" + groupName;

		Stat status = znode_exists(path, watchFlag);

		if (status != null) {
			zkeeper.delete(path, -1);
		}
	}

	public List<String> listGroupChildren(String groupName) throws KeeperException {
		String path = "/" + groupName;
		
		try {
			return zkeeper.getChildren(path, true).stream().sorted((s1,s2) -> s1.compareToIgnoreCase(s2)).collect(Collectors.toList());
		} catch (InterruptedException e) {
			throw KeeperException.create(Code.APIERROR, groupName);
		}
	}

	public Stat znode_exists(String path, boolean watch) throws InterruptedException, KeeperException {
		return zkeeper.exists(path, watch);
	}

	public void reset(String group) throws KeeperException, InterruptedException {
		List<String> children = listGroupChildren(group);
		
		if(children != null && children.size() > 0){
			for (String string : children) {
				reset(group+"/"+string);	
			}
		}		
		deleteGroup(group, false);
	}

	public States getState() {
		return zkeeper.getState();
	}		
}
