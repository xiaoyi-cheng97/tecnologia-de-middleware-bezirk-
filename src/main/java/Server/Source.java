package Server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.commons.lang3.StringUtils;

import Zookeeper.ZKManager;

public class Source{
	public static final String SOURCE_FOLDER_PATHNAME = "source_folder";
	public static final String SOURCE_NODE_PATHNAME = "source";
	
	private ZKManager zoo;
	private List<String> view;
	private DatagramSocket socket;
	
	private int id;
	private String ip;
	private int port;
	private String caminho;
	
	private Timer t;
	
	private int messagesSent;
	
	public Source() {
		initSourceNode();
		t = new Timer();
		VerifyTask v = new VerifyTask(this, this.zoo);
		t.schedule(v, 2000);
		broadcastPackets();
	}

	private void initSourceNode() {
		try {
			//creates a Manager instance
			zoo = new ZKManager();
			
			
			while (zoo.getState() != ZooKeeper.States.CONNECTED) {
				Thread.sleep(1000);
			}
				
			//checks if the source folder exists
			Stat s = zoo.znode_exists("/" + SOURCE_FOLDER_PATHNAME, false);
			
			//if not, creates one
			if(s == null) {
				zoo.createGroup(SOURCE_FOLDER_PATHNAME, false);
			}
			
			this.ip = InetAddress.getLocalHost().getHostAddress();
			
			Random portR = new Random();
			
			this.port = portR.nextInt(4000) + 2000;
			
			byte[] data = (ip+":"+port).getBytes();
			
			//joins source folder 
			this.caminho = zoo.joinGroup(SOURCE_FOLDER_PATHNAME, SOURCE_NODE_PATHNAME, data, false, true, true);
			System.out.println(caminho);
			//creates its own folder for network management
			this.id = getServiceNumberFromPath(caminho);
			
			if(zoo.znode_exists("/"+ SOURCE_NODE_PATHNAME + id, false) == null) {	
				zoo.createGroup(SOURCE_NODE_PATHNAME + id, true);
			}
			
			else {
				resetSource(SOURCE_NODE_PATHNAME + id);
				zoo.createGroup(SOURCE_NODE_PATHNAME + id, true);
			}
		
		} catch (IOException | InterruptedException | KeeperException e) {
			
			e.printStackTrace();
		}
	}
	
	private void broadcastPackets()  {
		try {
			socket = new DatagramSocket();
			int frame = 0;
			
			while(true) {
				Thread.sleep(50);
				String msg = "Source Node:" + id + " frame num:" + frame;
				 byte[] data = msg.getBytes();
				 DatagramPacket pack = new DatagramPacket(data, data.length);
				 
				 if (view != null && !view.isEmpty()) {
					 for (String child : view) {
							String[] member = child.split("/");
							String[] info = member[member.length-1].split(":");
							InetAddress add = InetAddress.getByName(info[0]);
							pack.setAddress(add);
							pack.setPort(Integer.valueOf(info[1]));
							try{
								socket.send(pack);
								messagesSent++;
							}
							catch(IOException e){
								System.out.println(e);
								continue;
							}
					 }
				 }
				 frame++;
			}

		} 
		catch (IOException e) {	
			e.printStackTrace();
		} 
		catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	public int getID() {
		return this.id;
	}

	public void updateClientData(List<String> clientData) {
		view = new ArrayList<>(clientData);
	}
	
	private void resetSource(String group){
		try {
			zoo.reset(group);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static int getServiceNumberFromPath(String path) {
		String numberStg = StringUtils.substringAfterLast(path,(SOURCE_NODE_PATHNAME + "-"));
		return Integer.parseInt(numberStg);
	}
	
	public void resetTimer() {
		t.cancel();
		t = new Timer();
		VerifyTask v = new VerifyTask(this, this.zoo);
		t.schedule(v, 2000);
		System.out.println("Mensagens enviadas: " + messagesSent);
	}

	
}