package ClusteringEvaluator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Class dedicated to a first story detection process on a tweets corpus
 * @author Arnaud and Remi
 */
public class FSDBuilder
{
    /** Cosine similarity threshold from which a vector is accepted in a cluster */
    private static final double COSINE_SIMILARITY_ACCEPTANCE = 0.1;
    
    /** Results file paths */
    private static final String FSD_SEEDS_FILE_PATH = "results\\fsdSeeds";
    public static final String FSD_CLUSTERING_FILE_PATH = "results\\fsdClustering";
    
    
    /**
     * Process a first story detection algorithm in order to clusters the given tweets, then exports it
     * @param uniqueTerms A set (unique elements) of every term found in the tweets
     * @param freqMatrix For each tweet, a terms vector. This terms vector contains, for each term, the number of times it appears in the tweet
     * @throws IOException 
     */
    public static void runFSDClustering(HashSet<String> uniqueTerms, HashMap<String, HashMap<String, Long>> freqMatrix) throws IOException
    {
        HashMap<Integer, String> clusterSeeds = new HashMap<Integer, String>();
        HashMap<Integer, List<String>> fsdClustering = new HashMap<Integer, List<String>>();
        
        getFSDClustering(uniqueTerms, freqMatrix, fsdClustering, clusterSeeds);
        exportFSDClustering(fsdClustering);
        exportFSDSeeds(clusterSeeds);
    }
   
    
    /**
     * Returns an FSD clustering using the cosine distance similarity between two terms vectors
     * Algorithm :
     * for each tweet terms vector
     *      for each cluster seed
     *          if the cosine similarity between the current tweet and the seed is superior or equal to 0.1
     *              Adding the tweet to the current cluster
     *              break
     *          end if
     *       end for
     *       if the tweet has not be added to any cluster
     *          Creating a new cluster with the current tweet as seed
     *       end if
     * end for
     * @param uniqueTerms A set of every unique terms contained by the tweets
     * @param freqMatrix A frequency matrix (See getTermsFrequencyMatrix)
     * @param fsdClustering An FSD clustering map :
     *                              Key = clusterId
     *                              Value = tweets ID list
     * @param clusterSeeds The map of each seed linked with its respective cluster :
     *                              Key = clusterId
     *                              Value = tweets ID seed
     */
    private static void getFSDClustering(HashSet<String> uniqueTerms, HashMap<String, HashMap<String, Long>> freqMatrix, HashMap<Integer, List<String>> fsdClustering, HashMap<Integer, String> clusterSeeds)
    {
        System.out.println("Building FSD clustering...");
        
        Integer currentClusterId = 1;
        Set<String> keySet = freqMatrix.keySet();
        int tweetsNumber = keySet.size();
        
        //Browsing each tweet ID
        for(String tweetId : keySet)
        {
            HashMap<String, Long> termsVector = freqMatrix.get(tweetId);
            
            //If the clusters list is empty
            if(fsdClustering.isEmpty())
            {
                //Creating a new cluster with the current tweet ID as seed
                addCluster(fsdClustering, tweetId, termsVector, clusterSeeds, currentClusterId);
                currentClusterId += 1;
            }
            else
            {
                boolean clustered = false;
                //Browsing the clusters ID
                for(Integer clusterId : clusterSeeds.keySet())
                {
                    //Getting the cluster seed
                    String seed = clusterSeeds.get(clusterId);
                    HashMap<String, Long> seedTermsVector = freqMatrix.get(seed);
                    Long[] frequencyVector = getFrequencyVector(uniqueTerms, termsVector);
                    Long[] seedFrequencyVector = getFrequencyVector(uniqueTerms, seedTermsVector);
                    
                    //Calculating the cosine similarity between the current tweet terms vector and the current cluster seed terms vector
                    if(getCosineSimilarity(frequencyVector, seedFrequencyVector) >= COSINE_SIMILARITY_ACCEPTANCE)
                    {
                        fsdClustering.get(clusterId).add(tweetId);
                        clustered = true;
                        break;
                    }
                }
                
                //If the tweet has not be added to any cluster
                if(!clustered)
                {
                    //Creating a new cluster with the current tweet ID as seed
                    addCluster(fsdClustering, tweetId, termsVector, clusterSeeds, currentClusterId);
                    currentClusterId += 1;
                }
            }
            
            //Displaying progression
            if(tweetsNumber % 100 == 0)
            {
                System.out.println(tweetsNumber + " tweets processed");
            }
        }
        
        System.out.println("Done");
    }
    
    /**
     * Returns a complete frequency vector with each term linked with its occurrence number in a specified tweet
     * @param uniqueTerms A set of every unique terms contained by the tweets 
     * @param termsVector A set of every terms present in a tweet linked with their occurrence number (1 or more)
     * @return A set of every terms present or not in a tweet linked with their occurrence number (0 or more)
     *          Each term in the uniqueTerms set is now present in the new terms vector
     */
    public static Long[] getFrequencyVector(HashSet<String> uniqueTerms, HashMap<String, Long> termsVector)
    {
        Long[] frequencyVector = new Long[uniqueTerms.size()];
        int i = 0;
        Long frequency;
        for(String key : uniqueTerms)
        {
            frequency = termsVector.get(key);
            if(frequency == null)
            {
                frequencyVector[i] = new Long(0);
            }
            else
            {
                frequencyVector[i] = frequency;
            }
            ++i;
        }
        return frequencyVector;
    }
    
    /**
     * Add a tweet ID to the FSD clustering and register this one as a seed of the new cluster
     * @param fsdClustering An FSD clustering map :
     *                              Key = clusterId
     *                              Value = tweets ID list
     * @param tweetId The tweet ID to add
     * @param termsVector A set of every unique terms contained by the tweets
     * @param clusterSeeds The map of each seed linked with its respective cluster :
     *                              Key = clusterId
     *                              Value = tweets ID seed
     * @param clusterId The new cluster ID
     */
    private static void addCluster(HashMap<Integer, List<String>> fsdClustering, String tweetId, HashMap<String, Long> termsVector, HashMap<Integer, String> clusterSeeds, Integer clusterId)
    {
        List<String> cluster = new ArrayList<String>();
        cluster.add(tweetId);
        fsdClustering.put(clusterId, cluster);
        clusterSeeds.put(clusterId, tweetId);
    }
    
    /**
     * Returns the cosine similarity between two vectors
     * A = (xa, ya), B = (xb, yb)
     * @param vector1 The first vector
     * @param vector2 The second vector
     * @return cosine(A, B) = A . B / ( ||A|| * ||B|| )
     *                = (xa * xb + ya * yb) / ( sqrt(xa * xa + ya * ya) * sqrt(xb * xb + yb * yb) )
     * 
     * Example :
     * A = (1, 0.5), B = (0.5, 1)
     * cosine(A, B) = (0.5 + 0.5) / sqrt(5/4) sqrt(5/4)
     *        = 4/5
     * Note : we are here working with longs, not float. The example above cannot work here.
     *          Thus, it explains how this function works.
     */
    public static double getCosineSimilarity(Long[] vector1, Long[] vector2)
    {
        double crossProduct = getCrossProduct(vector1, vector2);
        double norm1 = getVectorNorm(vector1);
        double norm2 = getVectorNorm(vector2);
        return crossProduct / (norm1 * norm2);
    }
    
    /**
     * Returns the cross product of two vectors
     * A = (xa, ya), B = (xb, yb)
     * @param vector1 The first vector
     * @param vector2 The second vector
     * @return crossProduct(A, B) = A . B
     *                      = (xa * xb + ya * yb)
     */
    private static double getCrossProduct(Long[] vector1, Long[] vector2)
    {
        double crossProduct = 0;
        for(int i = 0; i < vector1.length; ++i)
        {
            crossProduct += vector1[i] * vector2[i];
        }
        return crossProduct;
    }
    
    /**
     * Returns the norm of the given vector
     * A = (xa, ya)
     * @param vector the vector to calculate the norm
     * @return norm(A) = ||A||
     *                 = sqrt(xa * xa + ya * ya)
     */
    private static double getVectorNorm(Long[] vector)
    {
        double norm = 0;
        Long value;
        for(int i = 0; i < vector.length; ++i)
        {
            value = vector[i];
            norm += value * value;
        }
        return Math.sqrt(norm);
    }
    
   
    /**
     * Export the FSD cluster seeds in a text file
     * @param clusterSeeds The map of each seed linked with its respective cluster :
     *                              Key = clusterId
     *                              Value = tweets ID seed
     * @throws IOException 
     */
    private static void exportFSDSeeds(HashMap<Integer, String> clusterSeeds) throws IOException
    {
        System.out.println("Exporting FSD seeds...");
        FileWriter writer = new FileWriter(FSD_SEEDS_FILE_PATH);
        
        for(Integer key : clusterSeeds.keySet())
        {
            writer.append(key.toString());
            writer.append(" ");
            writer.append(clusterSeeds.get(key));
            writer.append("\n");
        }
        
        writer.flush();
        writer.close();
        System.out.println("Done");
    }
    
    /**
     * Export the FSD clustering in a text file
     * @param fsdClustering An FSD clustering map :
     *                              Key = clusterId
     *                              Value = tweets ID list
     * @throws IOException 
     */
    private static void exportFSDClustering(HashMap<Integer, List<String>> fsdClustering) throws IOException
    {
        System.out.println("Exporting FSD clustering...");
        FileWriter writer = new FileWriter(FSD_CLUSTERING_FILE_PATH);
        
        for(Integer key : fsdClustering.keySet())
        {
            for(String tweetId : fsdClustering.get(key))
            {
                writer.append(key.toString());
                writer.append(" ");
                writer.append(tweetId);
                writer.append("\n");
            }
        }
        
        writer.flush();
        writer.close();
        System.out.println("Done");
    }
}
