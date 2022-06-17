package Client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Timer;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import Server.Source;
import Zookeeper.ZKManager;

public class Client {
	private List<String> path;
	private List<String> view = new ArrayList<>();

	private ZKManager zoo;

	private int sourceId;

	private InetAddress ip;
	private int port;

	private Timer t;

	private DatagramSocket socket;
	private List<String> sourceNodes;
	private Scanner scan;
	String ogPath;

	private int lastFrame;
	private ArrayList<Integer> frames;
	private Map<String, Integer> parentsLastFrame = new HashMap<String, Integer>() ;
	
	int parents = 0;
	
	private byte[] data = new byte[256];

	private PacketThread p;
	
	private int messagesSent;
	private int messagesReceivedInOrder;

	public Client() {
		path = new ArrayList<>();
		initClientNode();
	}

	/**
	 * 
	 */
	private void initClientNode() {
		try {
			zoo = new ZKManager();
			
			while (zoo.getState() != ZooKeeper.States.CONNECTED) {
				Thread.sleep(1000);
			}
			
			scan = new Scanner(System.in);

			//get local address
			try(final DatagramSocket socket = new DatagramSocket()){
				socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
				this.ip = InetAddress.getByName(socket.getLocalAddress().getHostAddress());
			}

			Random portR = new Random();
			
			this.port = portR.nextInt(4000) + 2000;
			
			System.out.println("IP - " + this.ip);
			System.out.println("Port - " + this.port);

			//list sources
			sourceNodes = zoo.listGroupChildren(Source.SOURCE_FOLDER_PATHNAME);
			int nSources = sourceNodes.size();

			boolean c = true;
			boolean process = false;
			boolean firstTime = true;
			String joinGroupResponse = "";

			while(c) {
				boolean correct = false;

				if(!process) listChannels(); //nao esta a receber pacotes
				
				while(!correct) {
					if(scan.hasNextInt()) { //se tem um int aka vai querer ver uma live
						int newchannel = scan.nextInt();
						
											
						if (newchannel <= nSources && newchannel > 0) {
							correct = true; 
							process = true; 
							
							if (newchannel != sourceId+1) {
								
								firstTime = true;
								for (String p : path) {
									zoo.reset(p.substring(1));									
								}	
								path.clear();
							}
							sourceId = newchannel;
						}

						else {
							System.out.println("Introduza um numero valido");
						}
					}
					else if(scan.hasNextLine()) {
						switch(scan.next()) {
						case "p":
							if(p instanceof PacketThread && p.isAlive()) { 
								p.stopThread();
								correct = true; 
								process = false;  //vai parar a rececao de pacotes, 
							}
							break;
						case "q":
							c = false; //vai parar completamente
							correct = true;
							process = false; //nao tem de continuar a funcao depois disto
							break;
						} 							
					}
				}
				
				if(process) {			
					ogPath = "source"+getServiceNumberFromPath(sourceNodes.get(--sourceId));
					if (firstTime) {
						firstTime = false;
						getNodesParent();
					}

					lastFrame = 0;
					frames = new ArrayList<>();
					System.out.println(path.toString());

					t = new Timer();
					VerifyClient v = new VerifyClient(this, this.zoo);
					t.schedule(v, 2000);
					p = new PacketThread();
					p.start();
				}
			}	
		} catch (IOException | InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	private void listChannels() {
		System.out.println("");
		System.out.println("Escolha a live que deseja ver: ");

		for (String s : sourceNodes) {
			System.out.print(s+" ");
		}
		System.out.println("");
	}

	private static void updateList(String[] names, String initial, List<String> update){
		for (String string : names) {
			update.add(initial+"/"+string);
		}
	}

	public List<String> getPath(){
		return path;
	}

	public void updateClientData(List<String> clientData) {
		
		view = new ArrayList<>(clientData);
		
	}

	public static int getServiceNumberFromPath(String path) {
		String numberStg = StringUtils.substringAfterLast(path,("source-"));
		return Integer.parseInt(numberStg);
	}

	public void resetTimer() {
		t.cancel();
		t = new Timer();
		VerifyClient v = new VerifyClient(this, this.zoo);
		t.schedule(v, 3000);
		System.out.println("Mensagens enviadas: " + messagesSent);
		System.out.println("Mensages recebidas por ordem: " + messagesReceivedInOrder);
	}
	
	public void getNodesParent() throws KeeperException, InterruptedException {
		byte[] data = (ip+":"+port).getBytes();
		
		String joinGroupResponse = zoo.joinGroup(ogPath, "client" + this.port, data, true, false, false);
		
		if (!joinGroupResponse.contains(":")) {
			path.add(joinGroupResponse);
		}

		else {
			List<String> possibleParents = new ArrayList<>();
			Random r = new Random();
			String [] sourceChildren = joinGroupResponse.split(":")[1].split(",");

			updateList(sourceChildren, ogPath, possibleParents);

			while (parents < 3) {
				int rand = r.nextInt(possibleParents.size());
				String group = possibleParents.get(rand);

				String joinSourceChildrenResponse = zoo.joinGroup(group, "client" + port, data, true, false, false);

				System.out.println(group);
				System.out.println(path);
				if (!path.contains(group)) {
					if (joinSourceChildrenResponse.split(":")[0] != "Full") {
						path.add(group);
						parents ++;
					}

					else {
						String[] children = joinSourceChildrenResponse.split(":")[1].split(",");
						updateList(children, group, possibleParents);

					}
				}
				
				possibleParents.remove(rand);
			}
		}
	}

	class PacketThread extends Thread {
		boolean parar = false;

		public void run() {
			try {
				socket = new DatagramSocket(port);
				
				while(!parar) {
					//saber id dos nodes pais
					ArrayList<Integer> parentNodesId = new ArrayList<>();
					
					for (String f : path) {
						String[] fList = f.split("/");
						parentNodesId.add(Integer.valueOf(fList[fList.length-1].substring(6, 10)));
					}					
					
					//receber mensagem
					DatagramPacket rPack = new DatagramPacket(data, data.length);
					socket.receive(rPack);

					String msg = new String(rPack.getData(), 0, rPack.getLength());
					
					String messageSource = msg.split(":")[1].split(" ")[0];
					String sourceNum = sourceNodes.get(sourceId).split("-")[1].replaceFirst("^0+(?!$)", "");
					int currentFrame = Integer.valueOf(msg.split(":")[2]);
					
					if (sourceNum.equals(messageSource)) {
						if (msg.charAt(0) == 'c') { //verifica se msg vem de nos clientes	
							parentsLastFrame.put(msg.substring(1,5), currentFrame);
							
							int maxFrame = Collections.max(parentsLastFrame.values());
							
							for (Map.Entry<String, Integer> entry : parentsLastFrame.entrySet()) {
							    if (entry.getValue() < maxFrame - 19) {
							    	System.out.println(parentNodesId.toString());
							    	
							     	path.remove(parentNodesId.indexOf(Integer.valueOf(entry.getKey())));
							    	parentsLastFrame.remove(entry);
							    	parents--; //baixar valor dos parents e detetar no ciclo infinito
							    	getNodesParent();
							    }
							}
							//verificar qual a maior frame e remover todos aqueles 10 frames abaixo abaixo
						}
						
						
						if (lastFrame == 0) {
							System.out.println(msg);					
							lastFrame = currentFrame;					
						}

						if (currentFrame == lastFrame+1) {
							System.out.println(msg);					
							lastFrame = currentFrame;
							messagesReceivedInOrder++;
						}

						if (frames.size() == 5)
							frames.remove(0);

						frames.add(currentFrame);

						Collections.sort(frames);
						
						//espalhar mensagem atraves de gossip
						String updatedMsg = "c" + port + msg;
						Random rTime = new Random();
						Thread.sleep(rTime.nextInt(1000));
						
						byte[] data = updatedMsg.getBytes();
						DatagramPacket sPack = new DatagramPacket(data, data.length);

						if(view != null) {
							synchronized(view) {
								List<String> copy = view;
								Collections.shuffle(copy);
								int i = 0;
								
								for (String child : copy) {
									if(i > (view.size()/2)) {
										String[] member = child.split("/");
										String[] info = member[member.length-1].split(":");
										
										InetAddress add = InetAddress.getByName(info[0]);
										
										sPack.setAddress(add);
										sPack.setPort(Integer.valueOf(info[1]));
										try {
											socket.send(sPack);
											messagesSent++;
											i++;
										}catch(IOException e) {
											continue;
										}
									}
								}
							}
						}
					}					
				}
			} catch (IOException | KeeperException | InterruptedException  e) {
				e.printStackTrace();
			}
		}

		public void stopThread() {
			parar = true;
			socket.disconnect();
			socket.close();
		}
	}
}
