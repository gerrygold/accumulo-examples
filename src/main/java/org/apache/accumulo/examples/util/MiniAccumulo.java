package org.apache.accumulo.examples.util;

import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MiniAccumulo {

	private static String zoohost = null;
	private static String zoohostFilePath = System.getProperty("java.io.tmpdir") + File.separatorChar + "mac.tmp";;
	
	private static Object monitor = new Object();

	public static void main(String[] args) throws IOException, InterruptedException {
		
		Logger.getRootLogger().setLevel(Level.WARN);

		Boolean daemon = "true".equalsIgnoreCase(System.getProperty("daemon"));

		// run in Accumulo MAC
		File tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDir, "password");
		accumulo.start();
		
		System.out.println("starting up ...");
		Thread.sleep(3000);
		
		File instFile = new File(zoohostFilePath);
		instFile.deleteOnExit();
		System.out.println("cluster running with instance name " + accumulo.getInstanceName() + " and zookeepers " + accumulo.getZooKeepers());
		
		try (FileWriter writer = new FileWriter(instFile)) {
			writer.write(accumulo.getZooKeepers());
		}
		
		if (daemon) {
			System.out.println("running as a deamon, ctrl-c or kill to shutdown");
			Runtime.getRuntime().addShutdownHook( new Thread() {
					    public void run() {
						/*
						try {
							MiniAccumulo.accumulo.stop();	
						} catch (Exception ex)
						{}
						*/
					      synchronized(monitor) {
						   monitor.notifyAll();
					      }
    					}
                                }
			);
			
			synchronized(monitor) {
			  try {
				    monitor.wait();
			  } catch(InterruptedException e) { }
			}
			
		} else {
			System.out.println("hit Enter to shutdown ..");
			System.in.read();
		}

		System.out.println("...shuting down ..");
		accumulo.stop();
	}

	public static String getZooHost() throws FileNotFoundException, IOException {
		if(zoohost == null) {
			try (BufferedReader reader = new BufferedReader(new FileReader(new File(zoohostFilePath)))) {
				zoohost = reader.readLine().trim();
			}
		}
		
		return zoohost;
	}
}
