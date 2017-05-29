package com.beproject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


public class ReadFromKafka {
	public double runs;
	public double wickets;
	public static int rn=0,wn=0,rnminus1=0,wnminus1=0;
	public static ArrayList<Integer> predscores= new ArrayList<Integer>();
	public static ArrayList<Integer> predsegments= new ArrayList<Integer>();
	public ReadFromKafka(double r, double w)
	{
		this.runs=r;
		this.wickets=w;
	}
	static double eucl_dist(double[] x, double[] y)
	{
		double dist=0;
		for(int i=0; i<9; i++)
		{
			dist=dist+Math.pow(x[i]-y[i],2);
		}
		return Math.pow(dist,0.5);
	}
	static ReadFromKafka knn(double[][] train, double[] test, int size_train)
	{
		int num_neigh=5;
		int min_index[]=new int[num_neigh];
		ArrayList<Double> dist_neigh=new ArrayList<Double>();
		for(int i=0; i<size_train; i++)
		{
			dist_neigh.add(eucl_dist(train[i],test));
		}
		//System.out.println(dist_neigh);
		double average_runs=0;
		double average_wicks=0;
		for(int i=0; i<num_neigh; i++)
		{
			min_index[i]=dist_neigh.indexOf(Collections.min(dist_neigh));
			dist_neigh.set(min_index[i],99999.9);
			//System.out.println(min_index[i]);
			average_runs=average_runs+train[min_index[i]][9];
			average_wicks=average_wicks+train[min_index[i]][10];
		}
		dist_neigh.clear();
		return new ReadFromKafka(average_runs/5.0,average_wicks/5);
	}
	static String managespace(String team)
	{
		if(team.split(" ").length==2)
		{
			String msteam=team.split(" ")[0]+"%20"+team.split(" ")[1];
			return msteam;
		}
		return team;
	}
	public static void main(String[] args) throws Exception {
		// define team key map
		final Dictionary<String, String> teamanno = new Hashtable<String, String>();
		teamanno.put("India","0");
		teamanno.put("Sri Lanka","1");
		teamanno.put("New Zealand","2");
		teamanno.put("Australia","3");
		teamanno.put("Pakistan","4");
		teamanno.put("Bangladesh","5");
		teamanno.put("England","6");
		teamanno.put("West Indies","7");
		teamanno.put("South Africa","8");
		//final String teammap[]={"India","Sri Lanka","New Zealand","Australia","Pakistan","Bangladesh","England","West Indies","South Africa"};
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));

		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the rebelance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		messageStream.rebalance().map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;
			@Override
			public String map(String value) throws Exception {
				//String query = "INSERT INTO data1 (data_id, data)"+" VALUES(2,'"+value+"');";
				//Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
				//Session session = cluster.connect("test");
				//session.execute(query);
				double train_data[][]=new double[1][1];
				int num_lines = 0;
				try (BufferedReader br1 = new BufferedReader(new FileReader("/home/mausam/all_data.txt"))) {
					//System.out.println("hello");
					while (br1.readLine() != null) num_lines++;
					br1.close();
					BufferedReader br = new BufferedReader(new FileReader("/home/mausam/all_data.txt"));
		    			String line;
					int count=0;
					train_data=new double[num_lines][11];
	    			while ((line = br.readLine()) != null) {
	       				String[] parts = line.split(",");
					for(int i=0; i<11; i++)
					{
						train_data[count][i]=Double.parseDouble(parts[i+2].split(":")[1]);
					}
					count++;
	    			}
		    		br.close();
				}
				catch (FileNotFoundException ex)
				{
					System.out.println("File not found!");
				}
				catch (IOException ioex)
				{
					System.out.println("IO exception!");
				}
				//Scanner sc = new Scanner(System.in);
				//System.out.println("Enter test data:");
				String[] values=value.split(",");
				//System.out.println(values[0].split(":")[1]);
				//System.out.println(values[1].split(":")[1]);
				if(values[0].equals("done"))
				{
					File file = new File("/home/mausam/predscores.txt");
			        // creates the file
			        file.createNewFile();
			      
			        // creates a FileWriter Object
			        FileWriter writer = new FileWriter(file); 
			        System.out.println(predscores.size());
			        // Writes the content to the file
			        for(int i=0; i<predscores.size(); i++)
			        {
			        	int score=(int)predscores.get(i);
			        	int seg=(int)predsegments.get(i);
			        	//System.out.println(score);
			        	writer.write(seg+","+score+"\n");
			        	writer.flush();
			        }
			        writer.close();
					System.exit(0);
					return "End";
				}
				else if(values[0].equals("innbreak"))
				{
					rn=0;
					wn=0;
					rnminus1=0;
					wnminus1=0;
					File file = new File("/home/mausam/predscores.txt");
			        // creates the file
			        file.createNewFile();
			      
			        // creates a FileWriter Object
			        FileWriter writer = new FileWriter(file); 
			        System.out.println(predscores.size());
			        // Writes the content to the file
			        for(int i=0; i<predscores.size(); i++)
			        {
			        	int score=(int)predscores.get(i);
			        	int seg=(int)predsegments.get(i);
			        	//System.out.println(score);
			        	writer.write(seg+","+score+"\n");
			        	writer.flush();
			        }
			        writer.close();
			        predscores.clear();
			        predsegments.clear();
			        return "Pass";
				}
				else if(Integer.parseInt(values[0].split(":")[1])%5==0 && Integer.parseInt(values[1].split(":")[1])==1)
				{
					//System.out.println("hello");
					int curr_runs=Integer.parseInt(values[4].split(":")[1]);
					int curr_wicks=Integer.parseInt(values[5].split(":")[1]);
					rnminus1=rnminus1+rn;
					wnminus1=wnminus1+wn;
					rn=curr_runs-rnminus1;
					wn=curr_wicks-wnminus1;
					//System.out.println("Enter the start segment:");
					//int start_seg=Integer.parseInt(args[0]);
					String curr_team=values[2].split(":")[1];
					String team1=teamanno.get(values[2].split(":")[1]);
					String team2=teamanno.get(values[3].split(":")[1]);
					int start_seg=(Integer.parseInt(values[0].split(":")[1])/5)+1;
					double runs_eoi=(double)curr_runs;
					double wicks_eoi=(double)curr_wicks;
					double target=Double.parseDouble(values[9].split(":")[1]);
					double test_data[]=new double[10];
					test_data[0]=Double.parseDouble(team1);
					System.out.println("team1:"+test_data[0]);
					test_data[1]=Double.parseDouble(team2);
					System.out.println("team2:"+test_data[1]);
					test_data[2]=(double)rnminus1;
					System.out.println("r(n-1):"+test_data[2]);
					test_data[3]=(double)wnminus1;
					System.out.println("w(n-1):"+test_data[3]);
					test_data[4]=Double.parseDouble(values[6].split(":")[1]);
					System.out.println("bats1:"+test_data[4]);
					test_data[5]=Double.parseDouble(values[7].split(":")[1]);
					System.out.println("bats2:"+test_data[5]);
					test_data[6]=Double.parseDouble(values[8].split(":")[1]);
					System.out.println("venue:"+test_data[6]);
					test_data[7]=(double)rn;
					System.out.println("r(n):"+test_data[7]);
					test_data[8]=(double)wn;
					System.out.println("w(n):"+test_data[8]);
					test_data[9]=Double.parseDouble(values[9].split(":")[1]);
					System.out.println("target:"+test_data[9]);
					for(int i=start_seg; i<=10; i++)
					{
						ReadFromKafka res=knn(train_data,test_data,num_lines);
						//System.out.println("Predicted runs for segment "+(i)+": "+res.runs);
						//System.out.println("Predicted wickets for segment "+(i)+": "+(int)(res.wickets+1));
						test_data[2]=test_data[2]+test_data[7];
						test_data[3]=test_data[3]+test_data[8];
						test_data[7]=res.runs;
						test_data[8]=res.wickets;
						runs_eoi=runs_eoi+res.runs;
						wicks_eoi=wicks_eoi+(int)(res.wickets+1);
						if(target!=0 && runs_eoi>=target)
						{
							runs_eoi=target;
							break;
						}
						if(wicks_eoi>=9)
							break;
					}
					//System.out.println("EOI runs: "+runs_eoi);  
					predsegments.add(start_seg);
					System.out.println(predsegments);
					predscores.add((int)runs_eoi);
					System.out.println(predscores);
					String command="";
					//int balls_rem=0;
					BufferedWriter out = null;
					FileWriter fstream = new FileWriter("/home/mausam/crawlerlog.txt", true); //true tells to append data.
					out = new BufferedWriter(fstream);
					if(target!=0 && runs_eoi>=target)
					{
						out.write("Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nOvers: "+(start_seg-1)*5+"\nPredicted EOI runs: "+(int)runs_eoi+"\n"+curr_team+" will most probably win\n");
						curr_team=managespace(curr_team);
						command = "curl -i http://localhost:5000/social_media/handler/Current%20score%20for%20"+curr_team+"%20:"+curr_runs+"-"+curr_wicks+"%0AOvers:"+(start_seg-1)*5+"%0APredicted%20EOI%20runs:"+(int)runs_eoi+"%0A"+curr_team+"%20will%20most%20probably%20win/1";
						Runtime.getRuntime().exec(command);
						out.close();
						return "Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nOvers: "+(start_seg-1)*5+"\nPredicted EOI runs: "+(int)runs_eoi+"\n"+curr_team+" will most probably win";
					}
					else if(target!=0 && runs_eoi<target)
						//balls_rem=(50-(start_seg-1)*5)*6;
						if(target-runs_eoi>20)
						{
							out.write("Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nOvers: "+(start_seg-1)*5+"\nPredicted EOI runs: "+(int)runs_eoi+"\n"+curr_team+" will most probably lose\n");
							curr_team=managespace(curr_team);
							command = "curl -i http://localhost:5000/social_media/handler/Current%20score%20for%20"+curr_team+"%20:"+curr_runs+"-"+curr_wicks+"%0AOvers:"+(start_seg-1)*5+"%0APredicted%20EOI%20runs:"+(int)runs_eoi+"%0A"+curr_team+"%20will%20most%20probably%20lose/1";
							Runtime.getRuntime().exec(command);
							out.close();
							return "Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nOvers: "+(start_seg-1)*5+"\nPredicted EOI runs: "+(int)runs_eoi+"\n"+curr_team+" will most probably lose";
						}
						else
						{
							out.write("Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nOvers: "+(start_seg-1)*5+"\nPredicted EOI runs: "+(int)runs_eoi+"\n"+curr_team+" still has chance to win\n");
							curr_team=managespace(curr_team);
							command = "curl -i http://localhost:5000/social_media/handler/Current%20score%20for%20"+curr_team+"%20:"+curr_runs+"-"+curr_wicks+"%0AOvers:"+(start_seg-1)*5+"%0APredicted%20EOI%20runs:"+(int)runs_eoi+"%0A"+curr_team+"%20still%20has%20a%20chance%20to%20win/1";
							Runtime.getRuntime().exec(command);
							out.close();
							return "Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nOvers: "+(start_seg-1)*5+"\nPredicted EOI runs: "+(int)runs_eoi+"\n"+curr_team+" still has chance to win";
						}
					else if(target==0)
					{
						out.write("Current score for "+curr_team+" : "+curr_runs+"-"+curr_wicks+"\nPredicted EOI runs: "+(int)runs_eoi+"\nOvers: "+(start_seg-1)*5+"\n");
						curr_team=managespace(curr_team);
						command = "curl -i http://localhost:5000/social_media/handler/Current%20score%20for%20"+curr_team+"%20:"+curr_runs+"-"+curr_wicks+"%0APredicted%20EOI%20runs:"+(int)runs_eoi+"%0AOvers:"+(start_seg-1)*5+"/1";
						Runtime.getRuntime().exec(command);
						out.close();
						return "Current score for "+curr_team+" : "+curr_runs+"/"+curr_wicks+". Predicted EOI score: "+(int)runs_eoi+"\nOvers: "+(start_seg-1)*5;
					}
					else
					{
						out.close();
						return "None";
					}
					
						
				}
				else
				{
					return "";
				}
			}
		}).print();
		env.execute();
	}
}

