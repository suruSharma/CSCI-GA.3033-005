import java.util.*;
import java.io.*;
//Each row is a set of comma-separated values in the following format:
//c.userid,a.reviewWeight,b.reviewWeight,a,businessID,c.reviewWeight
class RowObject{
	public int rwFirstStage, rwFinalStage;
	//rwFirstStage=100-abs(a.reviewWeight-b.reviewWeight)
	public String userid,businessid;
	public RowObject(String row){
		StringTokenizer tokens = new StringTokenizer(row,",");
		userid = tokens.nextToken();
		rwFirstStage = 100-Math.abs(Integer.parseInt(tokens.nextToken())-Integer.parseInt(tokens.nextToken()));
		businessid= tokens.nextToken();
		rwFinalStage=Integer.parseInt(tokens.nextToken());
	}
}

public class Querying {
	public static void main(String args[]) throws IOException{
		String currentLine;
		BufferedReader br = new BufferedReader(new FileReader("analytics/analytictext3.txt"));
		Hashtable<String, ArrayList<RowObject> > rows= new Hashtable<String, ArrayList<RowObject>>();
		//for the hashtable c.userid is the primary key
		//but there are multiple rows with the same userid
		//so store the RowObject for all those multiple rows in value as ArrayList<RowObject>
		while((currentLine = br.readLine()) != null){
			RowObject obj= new RowObject(currentLine);
			if(rows.containsKey(obj.userid)){
				ArrayList<RowObject> temp = rows.get(obj.userid);
				rows.remove(obj.userid);
				temp.add(obj);
				rows.put(obj.userid, temp);
			}
			else{
				ArrayList<RowObject> temp = new ArrayList<RowObject>();
				temp.add(obj);
				rows.put(obj.userid, temp);
			}
		}
		//Hashtable is setup
		Set<String> keys = rows.keySet();
		//keys contains the set of all keys in the hashtable
		Vector<Double> similarityIndex = new Vector<Double>();
		//similarityIndex stores the unnormalized similarityIndexes for all users
		Vector<Double> rweight = new Vector<Double>();
		//rWeight stores the rwFinalStage for those corresponding users
	    for(String key: keys){
	    	ArrayList<RowObject> temp = new ArrayList<RowObject>();
	    	temp=rows.get(key);
	    	double val=0;
	    	for(int i =0 ; i < temp.size();i++){
	    		val+=temp.get(i).rwFirstStage;
	    	}
	    	rweight.add((double)temp.get(0).rwFinalStage);
	    	val=val/temp.size();
	    	similarityIndex.add(val);
	    }
	    double total=0;
	    for(int i = 0 ; i < similarityIndex.size(); i++){
	    	total+=similarityIndex.get(i);
	    }
	    for(int i = 0 ; i < similarityIndex.size(); i++){
	    	double t=similarityIndex.remove(0);
	    	double r=rweight.remove(0);
	    	rweight.add(r);
	    	t=t/total;
	    	similarityIndex.add(t);
	    }
	    //similarityIndex now contains the normalized similarityIndexes
	    double weight=0;
	    for(int i = 0 ; i < similarityIndex.size(); i++){
	    	weight+=similarityIndex.get(i)*rweight.get(i);
	    }
	    System.out.println("Recommendation is " + weight);
	    br.close();
	}
}
