package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.ContextWrapper;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

class MyContextWrapper extends ContextWrapper {
	public MyContextWrapper(Context base){
		super(base);
	}
	public String getPort(){
		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		return myPort;
	}
}

public class SimpleDynamoProvider extends ContentProvider {
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";

	public static boolean connected[] = new boolean[]{false, false, false, false, false};
	static Context context;

	public static String portNo = "";
	static Node myNode;

	public static TreeMap<String, Node> tv = new TreeMap<String, Node>();
	public static ArrayList<Node> nodeList = new ArrayList<Node>();

	static ArrayList<Node> getNodeList() {
		return nodeList;
	}

	static void setNodeList(ArrayList<Node> nodeList1) {
		nodeList = nodeList1;
	}

	static Node getMyNode() {
		return myNode;
	}

	static void setMyNode(Node node) {
		myNode = node;
	}

	public static void setTreeMap(TreeMap<String, Node> tv1) {
		tv = tv1;
	}

	public static TreeMap getTreeMap() {
		return tv;
	}

	@Override
	public int delete(Uri uri, final String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String fileName = selection;

		if (selection.equals("@")) {
			Log.v("File input stream", fileName);
			String[] fileList = getContext().fileList();
			Log.v("File input stream", Integer.toString(fileList.length));

			for (int i = 0; i < fileList.length; i++) {
				Log.v("File input stream", fileList[i]);
				try {
					fileName = fileList[i];
					getContext().deleteFile(fileName);
				} catch (Exception e) {
					Log.e("Exception Thrown", "Exception Thrown");
				}
			}
		} else if (selection.equals("*")) {
			sendDeleteReq();
		} else {
			try {

				File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
					public boolean accept(File pathname) {
						return pathname.getName().contains(selection);
					}
				}));

				for (int i = 0; i < listOfFiles.length; i++) {
					Log.v("File input stream", listOfFiles[i].getName());
					try {
						listOfFiles[i].delete();
					} catch (Exception e) {
						Log.e("Exception Thrown", "Exception Thrown");
					}
				}
			} catch (Exception e) {
				Log.e("Exception Thrown", "Exception Thrown");
			}
		}
		return 0;
	}

	void sendDeleteReq() {

		Log.v("ServerTask", "Sending delete node list");
		String remotePort[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

		for (int i = 0; i < 5; i++) {

			if (SimpleDynamoHelper.getConnected(remotePort[i])) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort[i]));

					String sendReq = "deleteReq";
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					out.writeUTF("aDel" + sendReq + "\n");
					out.flush();

				} catch (Exception ex) {
					Log.e("ServerTask", "Sending deleteReq to " + remotePort[i] + " fail");
					ex.printStackTrace();
				}
			} else {
				Log.e("ServerTask", "Sending deleteReq to " + remotePort[i] + " skipped");
			}
		}
		return;
	}

	public int deleteReq() {

		// TODO Auto-generated method stub
		String fileName = "";

		Log.v("File input stream", fileName);
		String[] fileList = getContext().fileList();
		Log.v("File input stream", Integer.toString(fileList.length));

		for (int i = 0; i < fileList.length; i++) {
			Log.v("File input stream", fileList[i]);
			try {
				fileName = fileList[i];
				context.deleteFile(fileName);
			} catch (Exception e) {
				Log.e("Exception Thrown", "Exception Thrown");
			}
		}
		return 0;
	}

	public int respondQuery1(final String filename) {
		String line = "";
		String filename1 = "";
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		try {
			Log.v("respondQuery1", filename);
			Log.v("File input stream", filename);
			File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
				public boolean accept(File pathname) {

					return pathname.getName().contains(filename);
				}
			}));
			Arrays.sort(listOfFiles);
			Log.v("respondQuery1", listOfFiles.toString());

			int lastIndex = listOfFiles.length;
			filename1 = listOfFiles[lastIndex - 1].getName();
			//.substring(0,listOfFiles[lastIndex-1].getName().length()-3);
			FileInputStream in = context.openFileInput(filename1);
			/*  Log.e(TAG, "File inputStreamReader.");*/
			InputStreamReader inputStreamReader = new InputStreamReader(in);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			StringBuilder sb = new StringBuilder();
			line = bufferedReader.readLine();
			sb.append(line);
			line = sb.toString();
			in.close();
			String[] result = {"Hi", filename1, line};
			Log.v("respondQuery1", filename1);
			Log.v("respondQuery1", line);
			reqQueMain.put(result);
		} catch (Exception e) {
			e.printStackTrace();
			Log.e(TAG, "File read failed...");
			String[] result = {"Hi", filename + "_v0", line};
			try {
				reqQueMain.put(result);
			} catch (Exception weq) {
				weq.printStackTrace();
			}
			//respondQuery1(filename);
			return 0;
		}
		return 1;

	}

	public void respondReplicaRequest(DataOutputStream outputStream, final String filename) {
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		String line = "";
		String filename1 = "";

		try {
			final String file_name = filename;
			File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getName().contains(file_name);

				}
			}));
			Log.v("respondQuery", myNode.node_id);
			Log.v(filename, filename);
			Arrays.sort(listOfFiles);
			int myVersion = listOfFiles.length - 1;
			filename1 = listOfFiles[myVersion].getName();
			FileInputStream in = context.openFileInput(listOfFiles[myVersion].getName());
			InputStreamReader inputStreamReader = new InputStreamReader(in);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			line = bufferedReader.readLine();


			Log.v("waiting", "waiting completed");
			Log.v("filename", filename);
			Log.v("value", line);


			String sendReq = "aDel" + "replicaResponse" + "aDel" + "1";
			outputStream.writeUTF(sendReq + "\n");
			outputStream.flush();
			outputStream.writeUTF("aDel" + filename1 + "aDel" + line + "\n");
			outputStream.flush();


		} catch (Exception e) {
			try {
				String sendReq = "aDel" + "replicaResponse" + "aDel" + "1";
				outputStream.writeUTF(sendReq + "\n");
				outputStream.flush();
				outputStream.writeUTF("aDel" + filename + "_v0" + "aDel" + line + "\n");
				outputStream.flush();

				e.printStackTrace();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	public void respondQuery(DataOutputStream outputStream,DataInputStream in, final String filename) {
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		String content = " ";
		try {
			String line = "";
			String filename1 = "";
			Node resNode = myNode.lookUp(genHash(filename));

			String args[] = {filename, "reqNode"};

			new ClientTask1(context).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, args);

			try {
				int myVersion = -1;

				int count = 1;
				String result[] = {};

				Log.v("respondQuery", Integer.toString(count));
				for (int i = 0; i < count; i++) {
					result = reqQue.take();
					while (!result[1].contains(filename)) {
						reqQue.put(result);
						result = reqQue.take();
					}
					Log.v("result[1]", result[1]);
					filename1 = result[1];
					Log.v("result[2]", result[2]);
					Log.v("myVersion", Integer.toString(myVersion));
					Log.v("myVersion", Character.toString(result[1].charAt(result[1].length() - 1)));
					//getResponsibleNode1(reqNode, myNode.succ.succ, filename, result[1], result[2]);
					sendFinalQuery1(outputStream,in,result[1],result[2]);
				}
				Log.v("waiting", "waiting completed");
				Log.v("filename", filename);
				content = result[2];
				Log.v(filename, content);

				Log.v("value", content);
			} catch (Exception e) {
				e.printStackTrace();
				sendFinalQuery1(outputStream,in,filename1,content);

				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}


	}

	public void respondCoordinateQuery(String reqNode, final String filename, String myfile, String myVal) {
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		String content = " ";
		try {
			String line = "";
			String filename1 = "";
			String args[] = {filename, reqNode};

			new ClientTask1(context).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, args);

			try {
				int myVersion = -1;
				int count = 2;

				String result[] = {"Hi", myfile, myVal};
				reqQue.put(result);
				Log.v("respondQuery", Integer.toString(count));
				for (int i = 0; i < count; i++) {
					result = reqQue.take();
					while (!result[1].contains(filename)) {
						reqQue.put(result);
						result = reqQue.take();
					}
					if (result[1].contains(filename)) {
						Log.v("myVersion", Character.toString(result[1].charAt(result[1].length() - 1)));
						if (myVersion <= Integer.parseInt(Character.toString(result[1].charAt(result[1].length() - 1)))) {
							myVersion = Integer.parseInt(Character.toString(result[1].charAt(result[1].length() - 1)));
							myfile = result[1];
							if ((!result[2].equals(" ")) && (!result[2].equals("")) && (!result[2].isEmpty()))
								content = result[2];
							else
								Log.v("result[2]", result[2].concat(":p:p"));

						}

					}
					Log.v("result[1]", result[1]);

					Log.v("result[2]", content);

				}
				Log.v("waiting", "waiting completed");
				Log.v("filename", filename);
				Log.v(filename, content);

				Log.v("value", content);

				sendFinalQuery(reqNode, myfile, content);


			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	void sendFinalQuery(String reqNode, String myfile, String content) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(reqNode));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			String sendReq = "aDel" + "queryResponse" + "aDel" + "1";
			out1.writeUTF(sendReq + "\n");
			out1.flush();
			String msg = in.readUTF();
			sendReq = "aDel" + myfile + "aDel" + content;
			out1.writeUTF(sendReq + "\n");
			out1.flush();
			Log.v("respondCoordinateQuery", sendReq + " " + reqNode);
		} catch (Exception e) {
			e.printStackTrace();
			sendFinalQuery(reqNode, myfile, content);
		}
	}

	void sendFinalQuery1(DataOutputStream outputStream,DataInputStream in, String myfile, String content) {
		try {
			/*Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(reqNode));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());*/
			String sendReq = "aDel" + "queryResponse" + "aDel" + "1";
			/*outputStream.writeUTF(sendReq + "\n");
			outputStream.flush();
			String msg = in.readUTF();*/
			sendReq = "aDel" + myfile + "aDel" + content;
			outputStream.writeUTF(sendReq + "\n");
			outputStream.flush();
			//Log.v("respondCoordinateQuery", sendReq + " " + reqNode);
		} catch (Exception e) {
			e.printStackTrace();
			sendFinalQuery1(outputStream,in, myfile, content);
		}
	}

	public void respondQueryTotal(String reqNode) {
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		String filename = "";
		String line = "";
		Log.v("File input stream", "respondQueryTotal");
		String[] fileList = context.fileList();
		Log.v("File input stream", Integer.toString(fileList.length));

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(reqNode));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			String sendReq = "aDel" + "queryResponseTotal" + "aDel" + Integer.toString(fileList.length) + "aDel";
			out1.writeUTF(sendReq + "\n");
			out1.flush();

			Log.v("File input stream", "queryResponseTotal");
			for (int i = 0; i < fileList.length; i++) {
				//	Log.v("File input stream", fileList[i]);

				try {
					filename = fileList[i];
					FileInputStream in = context.openFileInput(filename);
					/*  Log.e(TAG, "File inputStreamReader.");*/
					InputStreamReader inputStreamReader = new InputStreamReader(in);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					StringBuilder sb = new StringBuilder();
					line = bufferedReader.readLine();
					sb.append(line);
					in.close();
					line = sb.toString();
					out1.writeUTF(filename + "aDel" + line + "\n");
					out1.flush();
				} catch (Exception e) {
					Log.e(TAG, "File read failed...");
					e.printStackTrace();
				}


			}
		} catch (Exception e) {

		}

	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	synchronized public void insertOtherNode(String key, String value) {
		// TODO Auto-generated method stub
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		Log.v("insert", value);
		final String filename = key;
		String string = value;
		String hashedKey = "";
		Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
		FileOutputStream outputStream;

		File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().contains(filename);
			}
		}));
		int version = listOfFiles.length;


		try {//Context.MODE_PRIVATE
			String sendReq = "aDel+insertReplica+aDel" + filename + "aDel" + string + "aDel" + myNode.node_id;
			sendReplica(filename, string, myNode.succ, sendReq);
			sendReplica(filename, string, myNode.succ.succ, sendReq);
			System.out.println(filename);
			System.out.println(context);
			//context.deleteFile(filename);
			outputStream = context.openFileOutput(filename + "_v" + version, Context.MODE_PRIVATE);
			outputStream.write(string.getBytes());
			outputStream.flush();
			outputStream.close();
			updateMissedData(filename,string);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				outputStream = context.openFileOutput(filename + "_v" + version, Context.MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			Log.e(TAG, "File write failed");
		}
		return;
	}

	synchronized public void insertReplica(String key, String value) {
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		// TODO Auto-generated method stub
		final String filename = key;
		String string = value;
		String hashedKey = "";
		Context context = SimpleDynamoActivity.getAppContext();
		Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);


		File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().contains(filename);

			}
		}));
		int version = listOfFiles.length;

		FileOutputStream outputStream;
		try {//Context.MODE_PRIVATE
			Log.v("insertReplica", value);
			System.out.println(filename);
			//context.deleteFile(filename);
			outputStream = context.openFileOutput(filename + "_v" + version, Context.MODE_PRIVATE);
			outputStream.write(string.getBytes());
			outputStream.flush();
			outputStream.close();
			updateMissedData(key,value);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				Log.v(filename, string);
				outputStream = SimpleDynamoProvider.context.openFileOutput(filename + "_v" + version, Context.MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();
				updateMissedData(key,value);

			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	synchronized public void insertReplica1(Context context1, String key, String value,String modifiedTime) {
		// TODO Auto-generated method stub
		final String filename = key;
		String string = value;
		String hashedKey = "";
		Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);

		FileOutputStream outputStream;
		File file = new File(filename);

		File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().contains(filename);

			}
		}));
		int version = listOfFiles.length;
		Long time = 0L;

		if(version>0 ){
			time = listOfFiles[version - 1].lastModified();
		}
		String line = "";
		try {
			FileInputStream in = context1.openFileInput(listOfFiles[version - 1].getName());
			/*  Log.e(TAG, "File inputStreamReader.");*/
			InputStreamReader inputStreamReader = new InputStreamReader(in);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			StringBuilder sb = new StringBuilder();
			line = bufferedReader.readLine();
			sb.append(line);
			in.close();
		} catch (Exception e) {
			Log.e(TAG, "File read failed...");
			e.printStackTrace();
		}
		Log.v("insertReplica1",modifiedTime);
		int mVer = Character.valueOf(modifiedTime.charAt(1));
		Log.v("insertReplica1",modifiedTime);
		Log.v("insertReplica1","insertReplica1");
		Log.v(line,value);
		Log.v(Long.toString(time),modifiedTime);

		if(version <= mVer  && (!line.equals(value))) {

			try {//Context.MODE_PRIVATE
				Log.v("insertReplica", value);
				System.out.println(filename);
				System.out.println(getContext());
				//context.deleteFile(filename);
				outputStream = context1.openFileOutput(filename + "_v" + version, Context.MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();
				updateMissedData(key, value);
			} catch (Exception e) {
				e.printStackTrace();
				try {
					Log.v(filename, string);
					outputStream = context1.openFileOutput(filename + "_v" + version, Context.MODE_PRIVATE);
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();
					updateMissedData(key, value);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		}

	}

	static File missed_files_dir_0;
	static String path_0;
	static File directory_0;

	static File missed_files_dir_1;
	static String path_1;
	static File directory_1;

	static File missed_files_dir_2;
	static String path_2;
	static File directory_2;

	String getPath0() {
		return path_0;
	}

	String getPath1() {
		return path_1;
	}

	File getFile0() {
		return directory_0;
	}

	File getFile1() {
		return directory_1;
	}

	synchronized public void updateMissedData(String key, String value) {
		// TODO Auto-generated method stub

		Context context = SimpleDynamoActivity.getAppContext();
		if (true) {
			File directory = directory_0;
			String path = directory.getPath();

			Log.v("insertMissedData_New", path);
			Log.v("insert", value);
			String filename = key;
			String string = value;
			String hashedKey = "";
			Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
			FileOutputStream outputStream;
			File file = new File(path + "/" + filename);

			if (file.exists()) {
				file.delete();

				try {//Context.MODE_PRIVATE
					System.out.println(filename);
					System.out.println(context);


					outputStream = new FileOutputStream(new File(path + "/" + filename));
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();

				} catch (Exception e) {
					e.printStackTrace();
					try {
						outputStream = new FileOutputStream(new File(path + "/" + filename));
						outputStream.write(string.getBytes());
						outputStream.flush();
						outputStream.close();
					} catch (Exception e2) {
						e.printStackTrace();
					}
					Log.e(TAG, "File write failed");
				}

				Log.v("File input stream", path);

				Log.v("File input stream", "sendMissedData");
				String[] fileList = directory.list();
				Log.v("File input stream", Integer.toString(fileList.length));
				Log.v("sendMissedData", fileList.toString());
			}
		}

		if (true) {
			File directory = directory_1;
			String path = directory.getPath();
			Log.v("insertMissedData_New", path);
			Log.v("insert", value);
			String filename = key;
			String string = value;
			String hashedKey = "";
			Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
			FileOutputStream outputStream;
			File file = new File(path + "/" + filename);

			if (file.exists()) {
				file.delete();

				try {//Context.MODE_PRIVATE
					System.out.println(filename);
					System.out.println(context);

					outputStream = new FileOutputStream(new File(path + "/" + filename));
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();

				} catch (Exception e) {
					e.printStackTrace();
					try {


						outputStream = new FileOutputStream(new File(path + "/" + filename));
						outputStream.write(string.getBytes());
						outputStream.flush();
						outputStream.close();
					} catch (Exception e2) {
						e.printStackTrace();
					}
					Log.e(TAG, "File write failed");
				}
			}

			Log.v("File input stream", path);

			Log.v("File input stream", "sendMissedData");
			String[] fileList = directory.list();
			Log.v("File input stream", Integer.toString(fileList.length));
			Log.v("sendMissedData", fileList.toString());


		}
		if (true) {
			File directory = directory_2;
			String path = directory.getPath();
			Log.v("insertMissedData_New", path);
			Log.v("insert", value);
			String filename = key;
			String string = value;
			String hashedKey = "";
			Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
			FileOutputStream outputStream;
			File file = new File(path + "/" + filename);

			if (file.exists()) {
				file.delete();

				try {//Context.MODE_PRIVATE
					System.out.println(filename);
					System.out.println(context);

					outputStream = new FileOutputStream(new File(path + "/" + filename));
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();

				} catch (Exception e) {
					e.printStackTrace();
					try {


						outputStream = new FileOutputStream(new File(path + "/" + filename));
						outputStream.write(string.getBytes());
						outputStream.flush();
						outputStream.close();
					} catch (Exception e2) {
						e.printStackTrace();
					}
					Log.e(TAG, "File write failed");
				}
			}

			Log.v("File input stream", path);

			Log.v("File input stream", "sendMissedData");
			String[] fileList = directory.list();
			Log.v("File input stream", Integer.toString(fileList.length));
			Log.v("sendMissedData", fileList.toString());


		}


	}


	synchronized public void insertMissedData(String key,String value,String missed_node_id) {
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		// TODO Auto-generated method stub
		Log.v("insertMissedData_New1", missed_node_id);

		Context context = SimpleDynamoActivity.getAppContext();
		if(missed_node_id.contains(myNode.succ.node_id)){
			File directory = directory_0;
			String path = directory.getPath();

			Log.v("insertMissedData_New",path);
			Log.v("insert", value);
			String filename = key;
			String string = value;
			String hashedKey="";
			Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
			FileOutputStream outputStream;
			try {//Context.MODE_PRIVATE
				System.out.println(filename);
				System.out.println(context);
				File file = new File(path+"/"+filename);
				if(file.exists()){
					file.delete();
				}
				outputStream = new FileOutputStream(new File(path+"/"+filename));
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();

			} catch (Exception e) {
				e.printStackTrace();
				try{
					outputStream = new FileOutputStream(new File(path+"/"+filename));
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();
				}
				catch (Exception e2){
					e.printStackTrace();
				}
				Log.e(TAG, "File write failed");
			}

			Log.v("File input stream", missed_node_id);
			Log.v("File input stream", path);

			Log.v("File input stream", "sendMissedData");
			String[] fileList = directory.list();
			Log.v("File input stream", Integer.toString(fileList.length));
			Log.v("sendMissedData", fileList.toString());


			try {

				for (int i = 0; i < fileList.length; i++) {

					try {
						filename = fileList[i];
						Log.v("File input stream", path+"/"+filename);

						FileInputStream inputStream = new FileInputStream(new File(path+"/"+filename));
						//FileInputStream in = context.openFileInput(path+"/"+filename);
						/*  Log.e(TAG, "File inputStreamReader.");*/
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuilder sb = new StringBuilder();
						String line = bufferedReader.readLine();
						sb.append(line);
						inputStream.close();
						line = sb.toString();
						Log.v(filename,line);
					} catch (Exception e) {
						Log.e(TAG, "File read failed...");
						e.printStackTrace();
					}


				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}if(missed_node_id.contains(myNode.succ.succ.node_id)){
			File directory = directory_1;
			String path = directory.getPath();
			Log.v("insertMissedData_New",path);
			Log.v("insert", value);
			String filename = key;
			String string = value;
			String hashedKey="";
			Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
			FileOutputStream outputStream;
			try {//Context.MODE_PRIVATE
				System.out.println(filename);
				System.out.println(context);
				File file = new File(path+"/"+filename);
				if(file.exists()){
					file.delete();
				}
				outputStream = new FileOutputStream(new File(path+"/"+filename));
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();

			} catch (Exception e) {
				e.printStackTrace();
				try{


					outputStream = new FileOutputStream(new File(path+"/"+filename));
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();
				}
				catch (Exception e2){
					e.printStackTrace();
				}
				Log.e(TAG, "File write failed");
			}

			Log.v("File input stream", missed_node_id);
			Log.v("File input stream", path);

			Log.v("File input stream", "sendMissedData");
			String[] fileList = directory.list();
			Log.v("File input stream", Integer.toString(fileList.length));
			Log.v("sendMissedData", fileList.toString());


			try {

				for (int i = 0; i < fileList.length; i++) {

					try {
						filename = fileList[i];
						Log.v("File input stream", path+"/"+filename);

						FileInputStream inputStream = new FileInputStream(new File(path+"/"+filename));
						//FileInputStream in = context.openFileInput(path+"/"+filename);
						/*  Log.e(TAG, "File inputStreamReader.");*/
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuilder sb = new StringBuilder();
						String line = bufferedReader.readLine();
						sb.append(line);
						inputStream.close();
						line = sb.toString();
						Log.v(filename,line);
					} catch (Exception e) {
						Log.e(TAG, "File read failed...");
						e.printStackTrace();
					}


				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		if(missed_node_id.contains(myNode.pred.node_id)){
			File directory = directory_2;
			String path = directory.getPath();

			Log.v("insertMissedData_New",path);
			Log.v("insert", value);
			String filename = key;
			String string = value;
			String hashedKey="";
			Log.v("Created " + hashedKey, "with value " + string + "Before hash " + filename);
			FileOutputStream outputStream;
			try {//Context.MODE_PRIVATE
				System.out.println(filename);
				System.out.println(context);
				File file = new File(path+"/"+filename);
				if(file.exists()){
					file.delete();
				}
				outputStream = new FileOutputStream(new File(path+"/"+filename));
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();

			} catch (Exception e) {
				e.printStackTrace();
				try{
					outputStream = new FileOutputStream(new File(path+"/"+filename));
					outputStream.write(string.getBytes());
					outputStream.flush();
					outputStream.close();
				}
				catch (Exception e2){
					e.printStackTrace();
				}
				Log.e(TAG, "File write failed");
			}

			Log.v("File input stream", missed_node_id);
			Log.v("File input stream", path);

			Log.v("File input stream", "sendMissedData");
			String[] fileList = directory.list();
			Log.v("File input stream", Integer.toString(fileList.length));
			Log.v("sendMissedData", fileList.toString());


			try {

				for (int i = 0; i < fileList.length; i++) {

					try {
						filename = fileList[i];
						Log.v("File input stream", path+"/"+filename);

						FileInputStream inputStream = new FileInputStream(new File(path+"/"+filename));
						//FileInputStream in = context.openFileInput(path+"/"+filename);
						/*  Log.e(TAG, "File inputStreamReader.");*/
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuilder sb = new StringBuilder();
						String line = bufferedReader.readLine();
						sb.append(line);
						inputStream.close();
						line = sb.toString();
						Log.v(filename,line);
					} catch (Exception e) {
						Log.e(TAG, "File read failed...");
						e.printStackTrace();
					}


				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}

	}



	synchronized public void sendMissedData(String reqNode,DataOutputStream outputStream) {
		String filename = "";
		String line = "";
		Context context = SimpleDynamoActivity.getAppContext();

		File directory=new File("Junk") ;
		if(reqNode.equals(myNode.succ.node_id)){
			directory = directory_0;
		}
		if(reqNode.equals(myNode.succ.succ.node_id)){
			directory = directory_1;
		}
		if(reqNode.equals(myNode.pred.node_id)){
			directory = directory_2;
		}
		String path = directory.getPath();

		Log.v("File input stream", reqNode);
		Log.v("File input stream", path);

		Log.v("File input stream", "sendMissedData");
		String[] fileList = directory.list();
		Log.v("File input stream", Integer.toString(fileList.length));
		Log.v("sendMissedData", fileList.toString());


		try {
			String sendReq = "aDel"+"sendMissedData"+"aDel" + Integer.toString(fileList.length) + "aDel";
			outputStream.writeUTF(sendReq + "\n");
			outputStream.flush();
			FileInputStream inputStream;
			InputStreamReader inputStreamReader;
			BufferedReader bufferedReader;
			StringBuilder sb;



			for (int i = 0; i < fileList.length; i++) {
				Log.v("File input stream", path+"/"+filename);

				try {
					filename = fileList[i];
					final String fName = fileList[i];
					File f = new File(path+"/"+filename);
					File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
						public boolean accept(File pathname) {
							return pathname.getName().contains(fName);

						}
					}));
					int version = listOfFiles.length;
					inputStream = new FileInputStream(new File(path+"/"+filename));

					inputStreamReader = new InputStreamReader(inputStream);
					bufferedReader = new BufferedReader(inputStreamReader);
					sb = new StringBuilder();
					line = bufferedReader.readLine();
					sb.append(line);
					line = sb.toString();

					outputStream.writeUTF(filename+"aDel"+line +"aDel"+version+ "\n");
					outputStream.flush();
					inputStream.close();
					bufferedReader.close();
				} catch (Exception e) {
					Log.e(TAG, "File read failed...");
					e.printStackTrace();
				}


			}
			for (File tmpf : directory.listFiles()){
				Log.v("deleting "+tmpf.getName(),"deleting "+tmpf.getName());
				tmpf.delete();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}

	}
	static File defaultDir;


	@Override
	synchronized public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		Log.v("insert", values.toString());
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");


		}catch (InterruptedException ie){
			ie.printStackTrace();
		}
		final String filename = values.getAsString("key");
		String string = values.getAsString("value");
		String hashedKey="";
		Node responsibleNode;
		try{
			hashedKey = genHash(filename);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		File[] listOfFiles = defaultDir.listFiles((new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().contains(filename);
			}
		}));
		updateMissedData(filename,string);
		int version = listOfFiles.length;
		responsibleNode = myNode.lookUp(hashedKey);
		if ((myNode.node_id).equals(responsibleNode.node_id)) {
			String sendReq ="aDel+insertReplica+aDel"+filename+"aDel"+string+"aDel"+myNode.node_id;
			sendReplica(filename,string,myNode.succ,sendReq);
			sendReplica(filename,string,myNode.succ.succ,sendReq);
			Log.v("Created " + filename, "with value " + string );
			FileOutputStream outputStream;
			try {//Context.MODE_PRIVATE
				//getContext().deleteFile(filename);
				outputStream = getContext().openFileOutput(filename+"_v"+version, Context.MODE_PRIVATE);
				outputStream.write(string.getBytes());
				outputStream.flush();
				outputStream.close();
				updateMissedData(filename,string);
				/*if(myNode.node_id.equals("11120")){
					sendReplica(filename,string,Integer.toString(Integer.parseInt(myNode.node_id) + 4));
					sendReplica(filename,string,"11108");
				}
				else if(myNode.node_id.equals("11124")){
					sendReplica(filename,string,"11112");
					sendReplica(filename,string,"11108");
				}
				else
				{
					sendReplica(filename,string,Integer.toString(Integer.parseInt(myNode.node_id) + 4));
					sendReplica(filename,string,Integer.toString(Integer.parseInt(myNode.node_id) + 8));
				}*/

			} catch (Exception e) {
				e.printStackTrace();
				Log.e(TAG, "File write failed");
			}
		}
		else
			sendToResponsibleNode(filename,string,responsibleNode);
		return uri;
	}

	void sendReplica(String key, String value, Node node, String sendReq) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.node_id));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out1.writeUTF(sendReq+"\n");
			out1.flush();
			socket.setSoTimeout(200);
			String msgReceived = "";
			msgReceived = in.readUTF();
			Log.v("sent replica",sendReq + node.node_id);
		}
		catch (SocketTimeoutException e){
			e.printStackTrace();
			Log.e(key,value);
			if(sendReq.contains("insertMissedData"))
				sendReplica(key,value,node.pred,sendReq);
			else
				insertMissedData(key,value,node.node_id);


		}
		catch (EOFException ee){
			ee.printStackTrace();

			Log.e(key,value);
			if(sendReq.contains("insertMissedData"))
				sendReplica(key,value,node.pred,sendReq);
			else
				insertMissedData(key,value,node.node_id);


		}
		catch(Exception ex){
			Log.e(key,value);
			insertMissedData(key,value,node.node_id);


			ex.printStackTrace();
			Log.e("ClientTask","sendReplica Error");
		}

	}



	void sendToResponsibleNode(String key, String value, Node responsible_node) {
		try {

			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(responsible_node.node_id));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			String sendReq ="aDel+insertRequest+aDel"+key+"aDel"+value;
			out1.writeUTF(sendReq+"\n");
			out1.flush();
			Log.v("send Responsible Node","sent insert req " + responsible_node.node_id);
			socket.setSoTimeout(300);
			String msgReceived = "";
			msgReceived = in.readUTF();
			if(msgReceived.equals("Ack")){

			}

		}
		catch(SocketTimeoutException e){
			e.printStackTrace();
			Log.e("ServerTask","sendToResponsibleNode Error " +responsible_node.node_id );
			String sendReq ="aDel"+"insertReplica"+"aDel"+key+"aDel"+value+"aDel"+responsible_node.node_id;
			String sendReq1 ="aDel"+"insertMissedData"+"aDel"+key+"aDel"+value+"aDel"+responsible_node.node_id;
			if(responsible_node.succ.node_id.equals(myNode.node_id))
				insertMissedData(key,value,responsible_node.node_id);
			else{
				sendReplica(key,value,responsible_node.succ,sendReq1);
				//sendReplica(key,value,responsible_node.pred.pred,sendReq1);
			}


			if(responsible_node.succ.succ.node_id.equals(myNode.node_id)){
				insertReplica(key,value);
			}
			else{
				sendReplica(key,value,responsible_node.succ.succ,sendReq);
			}
			if(responsible_node.succ.node_id.equals(myNode.node_id)){
				insertReplica(key,value);
			}
			else{
				sendReplica(key,value,responsible_node.succ,sendReq);
			}
		}
		catch(EOFException ex){
			ex.printStackTrace();
			Log.e("ServerTask","sendToResponsibleNode Error " +responsible_node.node_id );
			String sendReq ="aDel"+"insertReplica"+"aDel"+key+"aDel"+value+"aDel"+responsible_node.node_id;
			String sendReq1 ="aDel"+"insertMissedData"+"aDel"+key+"aDel"+value+"aDel"+responsible_node.node_id;
			if(responsible_node.succ.node_id.equals(myNode.node_id))
				insertMissedData(key,value,responsible_node.node_id);
			else
				sendReplica(key,value,responsible_node.succ,sendReq1);

			if(responsible_node.succ.succ.node_id.equals(myNode.node_id)){
				insertReplica(key,value);
			}
			else{
				sendReplica(key,value,responsible_node.succ.succ,sendReq);
			}

			if(responsible_node.succ.node_id.equals(myNode.node_id)){
				insertReplica(key,value);
			}
			else{
				sendReplica(key,value,responsible_node.succ,sendReq);
			}

		}
		catch (Exception e){
			e.printStackTrace();
			Log.e("ServerTask","sendToResponsibleNode Error " +responsible_node.node_id );
			String sendReq ="aDel"+"insertReplica"+"aDel"+key+"aDel"+value+"aDel"+responsible_node.node_id;
			String sendReq1 ="aDel"+"insertMissedData"+"aDel"+key+"aDel"+value+"aDel"+responsible_node.node_id;
			if(responsible_node.succ.node_id.equals(myNode.node_id))
				insertMissedData(key,value,responsible_node.node_id);
			else
				sendReplica(key,value,responsible_node.succ,sendReq1);

			if(responsible_node.succ.succ.node_id.equals(myNode.node_id)){
				insertReplica(key,value);
			}
			else{
				sendReplica(key,value,responsible_node.succ.succ,sendReq);
			}

			if(responsible_node.succ.node_id.equals(myNode.node_id)){
				insertReplica(key,value);
			}
			else{
				sendReplica(key,value,responsible_node.succ,sendReq);
			}

		}
	}
	static BlockingQueue<String> waitingQue = new LinkedBlockingDeque<String>();
	@Override
	public boolean onCreate() {
		String hashedPort="";
		portNo = (new MyContextWrapper(getContext())).getPort();
		SimpleDynamoProvider.context = getContext();
		String remotePort[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
		try{
			Log.v(TAG,"Attempting to hash port no.");
			hashedPort = genHash(Integer.toString(Integer.parseInt(portNo)/2));
			Log.v(TAG + "portNo",portNo);
			Log.v(TAG,"Hash Success");
		}
		catch(Exception exception){
			if((hashedPort!=null && !hashedPort.isEmpty()) || (portNo!=null&& !portNo.isEmpty()))
				Log.v("PortNumber: "+portNo,hashedPort);
			else if(hashedPort!=null && !hashedPort.isEmpty())
				Log.v(TAG,"portNo is null");
			else if(portNo!=null || !hashedPort.isEmpty())
				Log.v(TAG,"hashedPort is null");
			Log.e("onCreate()","Node creation Exception");
			Log.e("exception",exception.toString());
		}

		myNode = new Node(portNo,hashedPort);
		Log.v("onCreate",getNodeList().toString());

		try {
			for (int i = 0; i < 5; i++) {
				{
					SimpleDynamoHelper.addNewNodeTreeMap(remotePort[i], genHash(Integer.toString(Integer.parseInt(remotePort[i]) / 2)));
					SimpleDynamoHelper.updateConnected(remotePort[i]);
				}

			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
		try{
			SimpleDynamoHelper.printNodeList(SimpleDynamoProvider.getNodeList());
			System.out.println(SimpleDynamoProvider.getMyNode().node_id);
			System.out.println(SimpleDynamoProvider.getMyNode().pred.node_id);
			System.out.println(SimpleDynamoProvider.getMyNode().succ.node_id);
		}catch(Exception e){
			e.printStackTrace();
		}
		try {
			Log.v(TAG, "Attempting to create a ServerSocket");
			ServerSocket serverSocket = new ServerSocket(10000);
			Log.v(TAG, "Creating server task");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.v(TAG, "ServerSocket created successfully");
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
			return false;
		}
		defaultDir = getContext().getFilesDir();
		setDirs(getContext());
		new ClientTask(getContext()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
		Log.v("onCreateEnd","onCreateEnd");
		return true;
	}
	void setDirs(Context context1){
		missed_files_dir_0 = context1.getDir(myNode.succ.node_id,Context.MODE_PRIVATE);
		path_0 = missed_files_dir_0.getAbsolutePath();
		directory_0 = new File(path_0);
		Log.v("File path_0", path_0);

		missed_files_dir_1 = context1.getDir(myNode.succ.succ.node_id,Context.MODE_PRIVATE);
		path_1 = missed_files_dir_1.getAbsolutePath();
		directory_1 = new File(path_1);
		Log.v("File path_1", path_1);

		missed_files_dir_2 = context1.getDir(myNode.pred.node_id,Context.MODE_PRIVATE);
		path_2 = missed_files_dir_2.getAbsolutePath();
		directory_2 = new File(path_2);
		Log.v("File path_2", path_2);

	}
	static BlockingQueue<String[]> reqQue = new LinkedBlockingDeque<String[]>();
	static BlockingQueue<String[]> reqQueMain = new LinkedBlockingDeque<String[]>();
	static BlockingQueue<ArrayList<String[]>> reqQueTotal = new LinkedBlockingDeque<ArrayList<String[]>>();

	@Override
	synchronized public Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder)  {
		// TODO Auto-generated method stub
		try {
			waitingQue.take();
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");
			waitingQue.put("Done");


		}catch (InterruptedException ie){
			ie.printStackTrace();
		}
		MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
		String filename = selection;
		String line = "";
		if (selection.equals("@")) {

			Log.v("File input stream", filename);
			String[] fileList = getContext().fileList();
			Log.v("File input stream", Integer.toString(fileList.length));

			for (int i = 0; i < fileList.length; i++) {
				//	Log.v("File input stream", fileList[i]);

				try {
					filename = fileList[i];
					FileInputStream in = getContext().openFileInput(filename);
					/*  Log.e(TAG, "File inputStreamReader.");*/
					InputStreamReader inputStreamReader = new InputStreamReader(in);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					StringBuilder sb = new StringBuilder();
					line = bufferedReader.readLine();
					sb.append(line);
					in.close();
				} catch (Exception e) {
					Log.e(TAG, "File read failed...");
					e.printStackTrace();
				}
				MatrixCursor.RowBuilder builder = matrixCursor.newRow();
				builder.add("key", filename.substring(0,filename.length()-3));
				builder.add("value", line);
				Log.v(filename, line);

			}
			matrixCursor.setNotificationUri(getContext().getContentResolver(), uri);


			return matrixCursor;


		}
		else if (selection.equals("*")) {

			try {
				int count = 0;
				sendQueryReq();
				Log.v("sendQueryReq",Integer.toString(connected.length));

				String[] fileList = getContext().fileList();
				Log.v("File input stream", Integer.toString(fileList.length));

				for (int i = 0; i < fileList.length; i++) {
					//	Log.v("File input stream", fileList[i]);

					try {
						filename = fileList[i];
						FileInputStream in = getContext().openFileInput(filename);
						/*  Log.e(TAG, "File inputStreamReader.");*/
						InputStreamReader inputStreamReader = new InputStreamReader(in);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuilder sb = new StringBuilder();
						line = bufferedReader.readLine();
						sb.append(line);
						in.close();
					} catch (Exception e) {
						Log.e(TAG, "File read failed...");
						e.printStackTrace();
					}
					MatrixCursor.RowBuilder builder = matrixCursor.newRow();
					builder.add("key", filename.substring(0,filename.length()-3));
					builder.add("value", line);
					Log.v(filename, line);

				}

				for(int i=0;i<connected.length;i++){
					if(connected[i] == true)
						count++;
				}
				Log.v("sendQueryReq",Integer.toString(count));

				ArrayList<String[]> temp;

				for(int i=0;i<count-1;i++){
					temp = reqQueTotal.take();
					Log.v("waiting","waiting completed");
					for(String[] result : temp){
						MatrixCursor.RowBuilder builder = matrixCursor.newRow();
						builder.add("key",result[0].substring(0,filename.length()-3) );
						builder.add("value", result[1]);
						Log.v(result[0],result[1]);

					}

				}

			} catch (Exception e) {
				Log.e(TAG, "File read failed...");
				e.printStackTrace();
			}
			matrixCursor.setNotificationUri(getContext().getContentResolver(), uri);

			return matrixCursor;


			//Log.v("query", selection);
		} else {
			int myVersion = -1;
			String hashedKey="";
			Node responsibleNode;
			try{
				hashedKey = genHash(filename);
			}
			catch(Exception e){
				e.printStackTrace();
			}

			responsibleNode = myNode.lookUp(hashedKey);
			if ((myNode.node_id).equals(responsibleNode.node_id)) {
				try {
					try {
						String content = " ";
						String file_name = "";
						String args[] = {filename,responsibleNode.node_id};
						new ClientTask1(context).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,args);
						getResponsibleNode2(responsibleNode.succ, filename);
						getResponsibleNode2(responsibleNode.succ.succ, filename);
						MatrixCursor.RowBuilder builder = matrixCursor.newRow();
						int count = 1;
						String result[] = {};

						Log.v("myVersion", Integer.toString(myVersion));
						if (SimpleDynamoHelper.getConnected(myNode.succ.node_id))
							count++;
						if (SimpleDynamoHelper.getConnected(myNode.succ.succ.node_id))
							count++;
						for (int i = 0; i < count; i++) {
							result = reqQueMain.take();
							Log.v("query " + filename, "Entering while loop");
							Log.v("query " + filename, "1251");

							while (!result[1].contains(filename)) {
								reqQueMain.put(result);
								result = reqQueMain.take();
							}
							Log.v("query " + filename, "Exiting while loop");
							Log.v("result[1]", result[1]);

							Log.v("result[2]", result[2]);
							if (result[1].contains(filename)) {
								Log.v("myVersion", Character.toString(result[1].charAt(result[1].length() - 1)));
								if (myVersion <= Integer.parseInt(Character.toString(result[1].charAt(result[1].length() - 1)))) {
									myVersion = Integer.parseInt(Character.toString(result[1].charAt(result[1].length() - 1)));
									file_name = result[1].substring(0, result[1].length() - 3);
									if(result[2].length()>5)
										content = result[2];
								}

							}
						}

						Log.v("waiting", "waiting completed");
						Log.v("filename", file_name);
						Log.v("value", content);

						builder.add("key", file_name);
						builder.add("value", content);
						matrixCursor.setNotificationUri(getContext().getContentResolver(), uri);

					} catch(Exception e){
						Log.e(TAG, "File read failed...");
						e.printStackTrace();
					}
				}catch (Exception w1){
					w1.printStackTrace();
				}
				return matrixCursor;

			}

			else{
				try {
					int count = 0;

					getResponsibleNode(responsibleNode,filename);
					if (SimpleDynamoHelper.getConnected(responsibleNode.node_id))
						count++;

					if(myNode.node_id.equals(responsibleNode.node_id)){
						respondQuery1(filename);

						count++;

					}else{
						getResponsibleNode(responsibleNode.succ,filename);
						if (SimpleDynamoHelper.getConnected(responsibleNode.succ.node_id))
							count++;
					}
					if(myNode.node_id.equals(responsibleNode.succ.succ.node_id)){
						respondQuery1(filename);

						count++;

					}else{
						getResponsibleNode(responsibleNode.succ.succ,filename);
						if (SimpleDynamoHelper.getConnected(responsibleNode.succ.succ.node_id))
							count++;
					}
					MatrixCursor.RowBuilder builder = matrixCursor.newRow();
					Log.v("waiting","waiting for data");
					String file_name = " ";
					String content = " ";

					String result[]  ={};
					Log.v("query "+filename, "Entering while loop");
					Log.v("query "+filename, "1303");
					for (int i = 0; i < count; i++) {
						result = reqQueMain.take();


						while (!result[1].contains(filename)) {
							reqQueMain.put(result);
							result = reqQueMain.take();
						}
						Log.v("query " + filename, "Exiting while loop");
						Log.v("result[1]", result[1]);

						Log.v("result[2]", result[2]);
						if (result[1].contains(filename)) {
							Log.v("myVersion", Character.toString(result[1].charAt(result[1].length() - 1)));
							if (myVersion <= Integer.parseInt(Character.toString(result[1].charAt(result[1].length() - 1)))) {
								myVersion = Integer.parseInt(Character.toString(result[1].charAt(result[1].length() - 1)));
								file_name = result[1].substring(0, result[1].length() - 3);
								if(result[2].length()>5)
									content = result[2];
							}

						}
					}
					/*while(!result[1].contains(filename)){
						//	Log.v("result[1]", result[1]+" "+filename );

						//reqQueMain.put(result);
						result  =reqQueMain.take();
					}*/
					Log.v("query "+filename, "Exiting while loop");

					Log.v("waiting","waiting completed");
					Log.v("result[1]", result[1]);

					Log.v("result[2]", result[2]);
					Log.v("myVersion", Integer.toString(myVersion));
					Log.v(result[1],result[2]);
					Log.v("filename",file_name);
					Log.v("value",content);
					builder.add("key",file_name );
					builder.add("value", content);
					matrixCursor.setNotificationUri(getContext().getContentResolver(), uri);
				} catch (Exception e) {
					Log.e(TAG, "File read failed...");
					e.printStackTrace();
				}

				return matrixCursor;


			}
		}
	}



	public void sendQueryReq(){
		Log.v("ServerTask", "Sending updated node list");
		String remotePort[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

		for (int i = 0; i < 5; i++) {
			sendQueryReq1(remotePort[i],i);

		}
		return;
	}
	void sendQueryReq1(String nodeId,int i){

		if(SimpleDynamoHelper.getConnected(nodeId) && (Integer.parseInt(nodeId) != Integer.parseInt(myNode.node_id))) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeId));

				String sendReq = "queryReqTotal"+"aDel"+myNode.node_id+"aDel";
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				out.writeUTF("aDel" + sendReq + "\n");
				out.flush();
				DataInputStream in = new DataInputStream(socket.getInputStream());

				try{
					socket.setSoTimeout(300);
					String messageReceived = in.readUTF();
				}
				catch (Exception e){

					//sendQueryReq1(nodeId);
					e.printStackTrace();
					connected[i] = false;

				}
				Log.e("ServerTask", "Sending queryReqTotal to " + nodeId + " success");

			} catch (Exception ex) {
				Log.e("ServerTask", "Sending queryReqTotal to " + nodeId + " fail");
				ex.printStackTrace();
			}
		}
		else
		{
			Log.e("ServerTask", "Sending queryReqTotal to " + nodeId + " skipped");
		}


		return;
	}

	public String getHash(String s) {
		try {
			return genHash(s);
		} catch (Exception e) {

		}
		return null;
	}
	synchronized public void getResponsibleNode(Node su,String filename){
		try {
			Log.v(myNode.node_id,myNode.succ.node_id);
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(su.node_id));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			String sendReq ="aDel"+"queryRequest"+"aDel"+filename+"aDel"+myNode.node_id+"aDel";
			out1.writeUTF(sendReq+"\n");
			out1.flush();
			socket.setSoTimeout(200);
			String msgReceived = in.readUTF();
			// msgReceived = in.readUTF();
		/*	out1.writeUTF("Ack" + "\n");
			out1.flush();*/
			Log.v("getResponsibleNode",msgReceived);
			SimpleDynamoHelper.updateConnected(su.node_id);
			msgReceived = in.readUTF();
			String result[] = msgReceived.split("aDel");
			Log.v("replicaResponse", result[1] + " " + result[2]);
			if(result[2].length() < 5){
				throw new Exception();
			}
			try {
				SimpleDynamoProvider.reqQueMain.put(result);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		catch (SocketTimeoutException e){
			e.printStackTrace();
			SimpleDynamoHelper.updateConnectedFalse(su.node_id);
			/*if(su.succ.node_id.equals(myNode.node_id))
				respondQuery1(filename);
			else
				getResponsibleNode(su.succ,filename);*/

		}
		catch(Exception ex){
			ex.printStackTrace();
			SimpleDynamoHelper.updateConnectedFalse(su.node_id);
		/*	if(su.succ.node_id.equals(myNode.node_id))
				respondQuery1(filename);
			else
				getResponsibleNode(su.succ,filename);*/
			Log.e("ClientTask","sendToResponsibleNode Error");
		}
		return;
	}

	synchronized public void getResponsibleNode2(Node su,String filename){
		try {

			Log.v(myNode.node_id,myNode.succ.node_id);
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(su.node_id));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			//socket.setSoTimeout(300);
			String sendReq ="aDel"+"replicaRequest"+"aDel"+filename+"aDel"+myNode.node_id+"aDel";
			out1.writeUTF(sendReq+"\n");
			out1.flush();
			socket.setSoTimeout(200);
			Log.v("send To R Node","sent replicaRequest"+myNode.node_id +" "+ su.node_id);

			String msgReceived = in.readUTF();
			msgReceived = in.readUTF();
			Log.v("getResponsibleNode2",msgReceived);
			SimpleDynamoHelper.updateConnected(su.node_id);
			msgReceived = in.readUTF();
			String result[] = msgReceived.split("aDel");
			Log.v("replicaResponse", result[1] + " " + result[2]);

			try {
				SimpleDynamoProvider.reqQueMain.put(result);
			} catch (Exception e) {
				e.printStackTrace();
			}

			Log.v("ServerTask Res", result[1] + " " + result[2]);
			Log.v("ServerTask", "Received queryResponse ");

		}
		catch (SocketTimeoutException e){
			e.printStackTrace();
			SimpleDynamoHelper.updateConnectedFalse(su.node_id);
			//getResponsibleNode2(su.pred,filename);
		}
		catch(Exception ex){
			ex.printStackTrace();
			SimpleDynamoHelper.updateConnectedFalse(su.node_id);
			//getResponsibleNode2(su.pred,filename);
//TODO: Failure case add getResponsibleNode2(pred) and increment count in query()
			Log.e("ServerTask","getResponsibleNode2 Error");
		}
		return;
	}

	public void getResponsibleNode1(String reqNode,Node responsible_node,String filename,String myFile, String myValue){

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(responsible_node.node_id));
			DataOutputStream out1 = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			String sendReq ="aDel"+"queryCoordinateRequest"+"aDel"+filename+"aDel"+reqNode+"aDel"+myFile+"aDel"+myValue;
			out1.writeUTF(sendReq+"\n");
			out1.flush();
			socket.setSoTimeout(200);
			String msgReceived = "";
			msgReceived = in.readUTF();
			Log.v("getResponsibleNode1","queryCoordinateRequest "+responsible_node.node_id+" "+myNode.node_id);
			SimpleDynamoHelper.updateConnected(responsible_node.node_id);

		}
		catch (SocketTimeoutException e){
			e.printStackTrace();
			SimpleDynamoHelper.updateConnectedFalse(responsible_node.node_id);
			if(responsible_node.pred.equals(myNode.node_id))
				respondQuery1(filename);
			else
				getResponsibleNode1(reqNode,responsible_node.pred,filename,myFile,myValue);
		}
		catch(Exception ex){
			ex.printStackTrace();
			SimpleDynamoHelper.updateConnectedFalse(responsible_node.node_id);
			if(responsible_node.pred.equals(myNode.node_id))
				respondQuery1(filename);
			else
				getResponsibleNode1(reqNode,responsible_node.pred,filename,myFile,myValue);

			Log.e("getResponsibleNode1","queryRequest Error");
		}
		return;
	}

	@Override
	synchronized public int update(Uri uri, ContentValues values, String selection,
								   String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
class ServerTask extends AsyncTask<ServerSocket, String, Void> {

	@Override
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];
		try {

			while (true) {
				try {
					Log.v("ServerTask", "serverS waiting for data");
					Socket serverS = serverSocket.accept();
					Log.v("ServerTask", "Data received");
					InputStream inputStream = serverS.getInputStream();
					DataInputStream in = new DataInputStream(inputStream);
					String msgReceived = "";
					msgReceived = in.readUTF();
					String receivedMessage = msgReceived;

					if (receivedMessage.isEmpty() || receivedMessage.equals("")) {
						Log.v("ServerTask", "Received msg empty");

					}

					Log.v("ServerTask", "receivedMessage from " + serverS.getInetAddress() + " " + receivedMessage);

					String missedNodeId = "", missedNodeHashId = "";
					String newNodeID = "", newNodeHashId = "";
					String nodeIds[];
					String fileName = "";
					if (receivedMessage.contains("missedDataRequest")) {
						try {
							nodeIds = receivedMessage.split("aDel");
							missedNodeId = nodeIds[2];
						} catch (Exception e) {
							Log.e("ServerTask", "EOFException");
							e.printStackTrace();
						}
						SimpleDynamoHelper.updateConnected(missedNodeId);
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						sdh.sendMissedData(missedNodeId,outputStream);

					}
					if (receivedMessage.contains("replicaRequest")) {
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();

						Log.v("ServerTask", "Received replicaRequest");
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						nodeIds = receivedMessage.split("aDel");
						fileName = nodeIds[2];
						String reqNode = nodeIds[3];


						/*if ((SimpleDynamoProvider.myNode.node_id).equals(reqNode)) {
							Log.e("ServerTask", "Received queryRequest to self");
							resulVal = sdh.respondQuery1(fileName);
							if(resulVal == 0){
								sdh.getResponsibleNode1(reqNode, responsibleNode, fileName);
							}

						}

						*//*else if ((SimpleDynamoProvider.myNode.node_id).equals(responsibleNode.node_id)) {
							sdh.getResponsibleNode1(reqNode, SimpleDynamoProvider.myNode.succ.succ, fileName);

						} *//*else*/ {
							sdh.respondReplicaRequest(outputStream, fileName);
						}

					}

					if (receivedMessage.contains("queryRequest")) {
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();

						Log.v("ServerTask", "Received queryRequest  ");
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						nodeIds = receivedMessage.split("aDel");
						fileName = nodeIds[2];
						String reqNode = nodeIds[3];
						String hashedKey = "";
						Node responsibleNode;
						try {
							hashedKey = sdh.getHash(fileName);
						} catch (Exception e) {
						}
						//responsibleNode = SimpleDynamoProvider.myNode.lookUp(hashedKey);
						Log.v(hashedKey, fileName);
						int resulVal = -1;

						/*if ((SimpleDynamoProvider.myNode.node_id).equals(reqNode)) {
							Log.e("ServerTask", "Received queryRequest to self");
							resulVal = sdh.respondQuery1(fileName);
							if(resulVal == 0){
								sdh.getResponsibleNode1(reqNode, responsibleNode, fileName);
							}

						}

						*//*else if ((SimpleDynamoProvider.myNode.node_id).equals(responsibleNode.node_id)) {
							sdh.getResponsibleNode1(reqNode, SimpleDynamoProvider.myNode.succ.succ, fileName);

						} *//*else*/ {
							sdh.respondQuery(outputStream,in, fileName);
						}

					}
					if (receivedMessage.contains("queryCoordinateRequest")) {
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();

						Log.v("ServerTask", "Received queryRequest  ");
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						nodeIds = receivedMessage.split("aDel");
						fileName = nodeIds[2];
						String reqNode = nodeIds[3];
						String coorFile = nodeIds[4];
						String coorVal = nodeIds[5];
						String hashedKey = "";
/*
						Node responsibleNode;
						try {
							hashedKey = sdh.getHash(fileName);
						} catch (Exception e) {
						}
						responsibleNode = SimpleDynamoProvider.myNode.lookUp(hashedKey);*/
						Log.v(hashedKey, fileName);
						//	int resulVal = -1;
						if(reqNode.equals(SimpleDynamoProvider.myNode.node_id)){
							String args[] = {fileName,coorFile,coorVal};
							new ClientTask2(SimpleDynamoProvider.context).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,args);
						}else{
							sdh.respondCoordinateQuery(reqNode, fileName,coorFile,coorVal);
						}
						/*if ((SimpleDynamoProvider.myNode.node_id).equals(reqNode)) {
							Log.e("ServerTask", "Received queryRequest to self");
							resulVal = sdh.respondQuery1(fileName);
							if(resulVal == 0){
								sdh.getResponsibleNode1(reqNode, responsibleNode, fileName);
							}

						}

						*//*else if ((SimpleDynamoProvider.myNode.node_id).equals(responsibleNode.node_id)) {
							sdh.getResponsibleNode1(reqNode, SimpleDynamoProvider.myNode.succ.succ, fileName);

						} *//*else*/

					}
					if (receivedMessage.contains("queryResponseTotal")) {
						ArrayList<String[]> arrayList = new ArrayList<String[]>();
						String numLines = receivedMessage.split("aDel")[1];
						String numLines1 = receivedMessage.split("aDel")[2];
						Log.v(numLines, numLines1);
						String result[] = {};
						for (int i = 0; i < Integer.parseInt(numLines1); i++) {
							receivedMessage = in.readUTF();
							result = receivedMessage.split("aDel");
							arrayList.add(result);
							Log.v("ServerTask Res", result[0] + " " + result[1]);

						}

						SimpleDynamoProvider.reqQueTotal.add(arrayList);
						Log.v("ServerTask", "Received queryResponseTotal ");

					} else if (receivedMessage.contains("queryResponse")) {
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						msgReceived = in.readUTF();
						Log.v(msgReceived,msgReceived);
						String result[] = msgReceived.split("aDel");
						try {
							SimpleDynamoProvider.reqQueMain.put(result);
						} catch (Exception e) {
							e.printStackTrace();
						}

						Log.v("ServerTask Res", result[1] + " " + result[2]);
						Log.v("ServerTask", "Received queryResponse ");

					} else if (receivedMessage.contains("replicaResponse")) {
						msgReceived = in.readUTF();
						String result[] = msgReceived.split("aDel");
						Log.v("replicaResponse", result[1] + " " + result[2]);

						try {
							SimpleDynamoProvider.reqQueMain.put(result);
						} catch (Exception e) {
							e.printStackTrace();
						}

						Log.v("ServerTask Res", result[1] + " " + result[2]);
						Log.v("ServerTask", "Received queryResponse ");

					}

					if (receivedMessage.contains("queryReqTotal")) {
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();
						String reqNodeId = msgReceived.split("aDel")[2];
						sdh.respondQueryTotal(reqNodeId);
						Log.v("ServerTask", "Received queryRequestTotal ");

					}
					else if (receivedMessage.contains("insertRequest")) {
						Log.v("ServerTask", "Received insertRequest ");

						nodeIds = msgReceived.split("aDel");
						String key = nodeIds[2];
						String value = nodeIds[3];
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();
						sdh.insertOtherNode(key, value);
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
					}
					else if (receivedMessage.contains("insertReplica")) {
						Log.v("ServerTask", "Received insertRequest ");
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						nodeIds = msgReceived.split("aDel");
						String key = nodeIds[2];
						String value = nodeIds[3];
						String host_node_ID = nodeIds[4];
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();
						sdh.insertReplica(key, value);

					}
					if (receivedMessage.contains("insertMissedData")) {
						Log.v("ServerTask", "Received insertRequest ");
						DataOutputStream outputStream = new DataOutputStream(serverS.getOutputStream());
						outputStream.writeUTF("Ack" + "\n");
						outputStream.flush();
						nodeIds = msgReceived.split("aDel");
						String key = nodeIds[2];
						String value = nodeIds[3];
						String missed_node_ID = nodeIds[4];
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();
						sdh.insertMissedData(key, value, missed_node_ID);

					}

					if (receivedMessage.contains("deleteRequest")) {
						SimpleDynamoProvider sdh = new SimpleDynamoProvider();
						sdh.deleteReq();
					}
					if (msgReceived.contains("updatedNodeList")) {
						Log.v("ServerTask", " before updatedNodeList");
						SimpleDynamoHelper.printNodeList(SimpleDynamoProvider.getNodeList());
						try {
							nodeIds = msgReceived.split("aDel");
							newNodeID = nodeIds[2];
							SimpleDynamoHelper.updateConnected(newNodeID);
							newNodeHashId = nodeIds[3];
							SimpleDynamoHelper.addNewNodeTreeMap(newNodeID, newNodeHashId);
						} catch (Exception ex) {
							Log.e("ServerTask", "Class Not found");
							ex.printStackTrace();
						}
						Log.v("ServerTask", " after updatedNodeList");
						SimpleDynamoHelper.printNodeList(SimpleDynamoProvider.getNodeList());
					}
					in.close();
				}catch (IOException e) {
					Log.e("ServerTask", "Server IO Exception");
					e.printStackTrace();
				}
			}
		}
		catch (Exception e) {
			Log.e("ServerTask", "Server IO Exception");
			e.printStackTrace();
		} finally {
			try {
				serverSocket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	protected void onProgressUpdate(String... strings) {
		String strReceived = strings[0].trim();

		ContentValues keyValueToInsert = new ContentValues();
		keyValueToInsert.put("value", strReceived);

	}
}

class ClientTask extends AsyncTask<String, Void, Void> {
	boolean flag1 = true;
	boolean flag2 = true;
	Context context1 ;
	public ClientTask(Context context){
		context1 = context;
	}
	void sendDataRequest() {
		try {
			SimpleDynamoProvider sdh = new SimpleDynamoProvider();
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDynamoProvider.getMyNode().pred.node_id));
			OutputStream outToServer;
			outToServer = socket.getOutputStream();
			DataOutputStream out = new DataOutputStream(outToServer);
			DataInputStream in = new DataInputStream(socket.getInputStream());
			String sendReq = "missedDataRequest";
			Log.v("ClientTask", "sending " + "missedDataRequest to "+ Integer.parseInt(SimpleDynamoProvider.getMyNode().pred.node_id));
			out.writeUTF("aDel" + sendReq + "aDel" + SimpleDynamoProvider.getMyNode().node_id + "aDel" + SimpleDynamoProvider.getMyNode().hashedId + "\n");
			out.flush();
			String receivedMessage="";
			receivedMessage = in.readUTF();
			socket.setSoTimeout(300);

			if (receivedMessage.contains("sendMissedData")) {
				ArrayList<String[]> arrayList = new ArrayList<String[]>();
				String numLines = receivedMessage.split("aDel")[1];
				String numLines1 = receivedMessage.split("aDel")[2];
				Log.v(numLines, numLines1);
				String result[] = {};
				for (int i = 0; i < Integer.parseInt(numLines1); i++) {
					receivedMessage = in.readUTF();
					result = receivedMessage.split("aDel");
					arrayList.add(result);
					try {
						SimpleDynamoProvider.waitingQue.put("Done");
					}catch (InterruptedException ie){
						ie.printStackTrace();
					}
					sdh.insertReplica1(context1,result[0],result[1],result[2]);
					Log.v("ServerTask Res", result[0] + " " + result[1]);
				}
			}
			Log.v("ClientTask End", "sendDataRequest End");
		}  catch (EOFException se1) {
			if(flag2)
				sendDataRequest1();
			try {
				Thread.sleep(3000);
			}catch (Exception e){
				e.printStackTrace();
			}
			if(flag1)
				sendDataRequest();
		}catch (Exception io) {
			Log.e("Exception Thrown", io.toString());
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
		}
		catch (StackOverflowError io) {
			Log.e("Exception Thrown", io.toString());
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
		}
		flag1 = false;
		return;
	}


	void sendDataRequest1() {
		try {
			SimpleDynamoProvider sdh = new SimpleDynamoProvider();
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDynamoProvider.getMyNode().pred.pred.node_id));
			OutputStream outToServer;
			outToServer = socket.getOutputStream();
			String nodeIds[], newNodeID, newNodeHashId;
			DataOutputStream out = new DataOutputStream(outToServer);
			String sendReq = "missedDataRequest";
			Log.v("ClientTask", "sending " + "missedDataRequest to "+ Integer.parseInt(SimpleDynamoProvider.getMyNode().pred.pred.node_id));				out.writeUTF("aDel" + sendReq + "aDel" + SimpleDynamoProvider.getMyNode().node_id + "aDel" + SimpleDynamoProvider.getMyNode().hashedId + "\n");
			out.flush();
			String receivedMessage="";
			DataInputStream in = new DataInputStream(socket.getInputStream());
			receivedMessage = in.readUTF();
			socket.setSoTimeout(300);

			if (receivedMessage.contains("sendMissedData")) {
				ArrayList<String[]> arrayList = new ArrayList<String[]>();
				String numLines = receivedMessage.split("aDel")[1];
				String numLines1 = receivedMessage.split("aDel")[2];
				Log.v(numLines, numLines1);
				String result[] = {};
				for (int i = 0; i < Integer.parseInt(numLines1); i++) {
					receivedMessage = in.readUTF();
					result = receivedMessage.split("aDel");
					arrayList.add(result);
					try {
						SimpleDynamoProvider.waitingQue.put("Done");
					}catch (InterruptedException ie){
						ie.printStackTrace();
					}
					sdh.insertReplica1(context1,result[0],result[1],result[2]);
					Log.v("ServerTask Res", result[0] + " " + result[1]);
				}
			}
			Log.v("ClientTask End", "sendDataRequest End");

		} catch (EOFException se2){
			if(flag1)
				sendDataRequest();
			try {
				Thread.sleep(3000);
			}catch (Exception e){
				e.printStackTrace();
			}
			if(flag2)
				sendDataRequest1();
		} catch (Exception io) {
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
			Log.e("Exception Thrown", io.toString());
		}catch (StackOverflowError io) {
			Log.e("Exception Thrown", io.toString());
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
		}
		flag2 = false;
		return;
	}


	void sendDataRequest2() {
		try {
			SimpleDynamoProvider sdh = new SimpleDynamoProvider();
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDynamoProvider.getMyNode().succ.node_id));
			OutputStream outToServer;
			outToServer = socket.getOutputStream();
			String nodeIds[], newNodeID, newNodeHashId;
			DataOutputStream out = new DataOutputStream(outToServer);
			String sendReq = "missedDataRequest";
			Log.v("ClientTask", "sending " + "missedDataRequest to "+ Integer.parseInt(SimpleDynamoProvider.getMyNode().succ.node_id));
			out.writeUTF("aDel" + sendReq + "aDel" + SimpleDynamoProvider.getMyNode().node_id + "aDel" + SimpleDynamoProvider.getMyNode().hashedId + "\n");
			out.flush();
			String receivedMessage="";
			DataInputStream in = new DataInputStream(socket.getInputStream());
			receivedMessage = in.readUTF();
			socket.setSoTimeout(300);

			if (receivedMessage.contains("sendMissedData")) {
				ArrayList<String[]> arrayList = new ArrayList<String[]>();
				String numLines = receivedMessage.split("aDel")[1];
				String numLines1 = receivedMessage.split("aDel")[2];
				Log.v(numLines, numLines1);
				String result[] = {};
				for (int i = 0; i < Integer.parseInt(numLines1); i++) {
					receivedMessage = in.readUTF();
					result = receivedMessage.split("aDel");
					arrayList.add(result);
					try {
						SimpleDynamoProvider.waitingQue.put("Done");
					}catch (InterruptedException ie){
						ie.printStackTrace();
					}
					sdh.insertReplica1(context1,result[0],result[1],result[2]);
					Log.v("ServerTask Res", result[0] + " " + result[1]);
				}
			}
			Log.v("ClientTask End", "sendDataRequest End");

		} catch (EOFException se2){
			if(flag1)
				sendDataRequest();
			try {
				Thread.sleep(3000);
			}catch (Exception e){
				e.printStackTrace();
			}
			if(flag2)
				sendDataRequest1();
		} catch (Exception io) {
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
			Log.e("Exception Thrown", io.toString());
		}catch (StackOverflowError io) {
			Log.e("Exception Thrown", io.toString());
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
		}
		flag2 = false;
		return;
	}



	@Override
	protected Void doInBackground(String... msgs) {
		try {
			Log.v("ClientTask", "ClientTask invoked1");
			sendDataRequest();
			sendDataRequest1();
			sendDataRequest2();
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
				SimpleDynamoProvider.waitingQue.put("Done");
				SimpleDynamoProvider.waitingQue.put("Done");
				SimpleDynamoProvider.waitingQue.put("Done");
				SimpleDynamoProvider.waitingQue.put("Done");
				SimpleDynamoProvider.waitingQue.put("Done");
				SimpleDynamoProvider.waitingQue.put("Done");

			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
			Log.v("doInBackground End", "doInBackground End");
		}catch (Exception e){
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
			e.printStackTrace();
		}catch (StackOverflowError io) {
			Log.e("Exception Thrown", io.toString());
			try {
				SimpleDynamoProvider.waitingQue.put("Done");
			}catch (InterruptedException ie){
				ie.printStackTrace();
			}
		}
		return null;
	}
}

class ClientTask1 extends AsyncTask<String, Void, Void> {

	Context context1 ;
	public ClientTask1(Context context){
		context1 = context;
	}

	@Override
	protected Void doInBackground(String... filename) {
		String line = "";
		String filename2 = "";
		String resNode = filename[1];
		try {
			final String file_name = filename[0];
			File[] listOfFiles = SimpleDynamoProvider.defaultDir.listFiles((new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getName().contains(file_name);

				}
			}));


			int myVersion = listOfFiles.length - 1;
			Arrays.sort(listOfFiles);
			if(myVersion>=0) {
				filename2 = listOfFiles[myVersion].getName();
				Log.v("ClientTask1",filename2);
				FileInputStream in = context1.openFileInput(listOfFiles[myVersion].getName());
				InputStreamReader inputStreamReader = new InputStreamReader(in);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				line = bufferedReader.readLine();
				String result[] = {"Hi",filename2,line};
				if(resNode.equals(SimpleDynamoProvider.myNode.node_id))
					SimpleDynamoProvider.reqQueMain.put(result);
				else
					SimpleDynamoProvider.reqQue.put(result);

			}else{
				String result[] = {"Hi",filename[0]+"_v0",line};
				if(resNode.equals(SimpleDynamoProvider.myNode.node_id))
					SimpleDynamoProvider.reqQueMain.put(result);
				else
					SimpleDynamoProvider.reqQue.put(result);

			}

		}catch (StackOverflowError io) {
			io.printStackTrace();
		}
		catch (Exception io1) {
			try {
				String result[] = {"Hi", filename[0]+"_v0", line};
				if(resNode.equals(SimpleDynamoProvider.myNode.node_id))
					SimpleDynamoProvider.reqQueMain.put(result);
				else
					SimpleDynamoProvider.reqQue.put(result);
			}catch (Exception ex){
				ex.printStackTrace();
			}
			io1.printStackTrace();
		}
		return null;
	}
}
class ClientTask2 extends AsyncTask<String, Void, Void> {

	Context context1 ;
	public ClientTask2(Context context){
		context1 = context;
	}

	@Override
	protected Void doInBackground(String... filename) {
		String line = filename[2];
		String filename2 = filename[1];
		String content = " ";
		try {
			final String file_name = filename[0];

			File[] listOfFiles = SimpleDynamoProvider.defaultDir.listFiles((new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getName().contains(file_name);

				}
			}));


			int myVersion = listOfFiles.length - 1;
			Arrays.sort(listOfFiles);
			if(myVersion>=Character.valueOf(filename[1].charAt(filename[1].length()-1))) {
				filename2 = listOfFiles[myVersion].getName();
				Log.v("ClientTask1",filename2);
				FileInputStream in = context1.openFileInput(listOfFiles[myVersion].getName());
				InputStreamReader inputStreamReader = new InputStreamReader(in);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				content = bufferedReader.readLine();
				if(!content.equals(" ")&&!content.equals(""))
					line = content;
			}
			String result[] = {"Hi",filename2,line};
			SimpleDynamoProvider.reqQueMain.put(result);
		}catch (StackOverflowError io) {
			io.printStackTrace();
		}
		catch (Exception io1) {
			try {
				String result[] = {"Hi", filename[0]+"_v0", line};
				SimpleDynamoProvider.reqQueMain.put(result);
			}catch (Exception ex){
				ex.printStackTrace();
			}
			io1.printStackTrace();
		}
		return null;
	}
}