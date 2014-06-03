/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ClusteringEvaluator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Main class dedicated to run and evaluate event detection clusterings
 * A first story detection is run, then we import a few clusterings, the real clustering
 * and we compare each clustering with the real one using the F-score method
 */
public class ClusteringEvaluator
{
    /** Clusters themes */
    private static final String CLUSTERS_THEMES_FILE_PATH = "resources\\clustersThemes";
    
    /** Clusterings to import */
    private static final String REAL_CLUSTERING_FILE_PATH = "resources\\tweetsClustered";
    private static final String KMEANS_CLUSTERING_FILE_PATH = "results\\kmeansClustering";
    
    /**
     * Algorithm :
     *      Calculating a terms frequency matrix regarding to each tweets content
     *      Calculating a FSD in order to cluster each tweet
     *      Exporting results
     * @param args Unused
     * @throws IOException
     * @throws org.apache.lucene.queryparser.classic.ParseException 
     */
    public static void main(String[] args) throws IOException, org.apache.lucene.queryparser.classic.ParseException
    {
        //========== BUILDING TWEETS FREQUENCY MATRIX - LUCENE ==========
        System.out.println("========== TWEETS FREQUENCY MATRIX ==========");
        
        //Variables
        HashMap<String, HashMap<String, Long>> tweetsFreqMatrix = new HashMap<String, HashMap<String, Long>>();
        HashSet<String> tweetsUniqueTerms = new HashSet<String>();
        
        //Building tweets frequency matrix
        FrequencyMatrixBuilder.getTermsFrequencyMatrix(tweetsFreqMatrix, tweetsUniqueTerms, null);
        FrequencyMatrixBuilder.exportFrequencyMatrix(tweetsFreqMatrix, tweetsUniqueTerms);
        
        
        
        //========== REAL CLUSTERING ==========
        System.out.println("========== REAL CLUSTERING ==========");
        
        //Variables
        HashSet<String> realClustersUniqueTerms = new HashSet<String>();
        HashMap<String, HashMap<String, Long>> realClustersThemesFreqMatrix = new HashMap<String, HashMap<String, Long>>();
        HashMap<Integer, List<String>> realClustering;
        
        //Importing clusterings
        realClustering = importClustering(REAL_CLUSTERING_FILE_PATH);
        System.out.println("Correct clustering number : " + realClustering.size());
        //Building real clusters themes frequency matrix
        HashMap<Integer, String> clusterThemes = importClusterThemes();
        List<HashMap<String, String>> clusterThemesCompatible = getMatrixFrequencyCompatibleThemes(clusterThemes);
        FrequencyMatrixBuilder.getTermsFrequencyMatrix(realClustersThemesFreqMatrix, realClustersUniqueTerms, clusterThemesCompatible);
        
        
        
        //========== K-MEANS CLUSTERING ==========
        System.out.println("========== K-MEANS CLUSTERING ==========");
        //Variables
        HashMap<Integer, HashMap<String, Long>> kmeansClustersFreqMatrix;
        HashMap<Integer, List<String>> kmeansClustering;
        HashMap<Integer, Integer> kmeansClustersLinked = null;
        double kmeansAccuracy;

        //Importing clusterings
        kmeansClustering = importClustering(KMEANS_CLUSTERING_FILE_PATH);
        //Building frequency matrix
        kmeansClustersFreqMatrix = getClustersFrequencyMatrix(kmeansClustering, tweetsFreqMatrix);
        //Linking real clusters themes with the algorithm clusters using the cosine similarity
        kmeansClustersLinked = linkRealClustersWithCorrectTweets(realClustering, kmeansClustersLinked, kmeansClustering);
        //Calculating clustering algorithm accuracy
        kmeansAccuracy = getClusteringAccuracy(realClustering, kmeansClustersLinked, kmeansClustering);
        System.out.println(String.format("K-means algorithm :\nAccuracy : %f\nClusters : %d", kmeansAccuracy, kmeansClustering.size()));
        
        
        //========== FSD CLUSTERING ==========
        System.out.println("========== FSD CLUSTERING ==========");
        //Variables
        HashMap<Integer, HashMap<String, Long>> fsdClustersFreqMatrix;
        HashMap<Integer, List<String>> fsdClustering;
        HashMap<Integer, Integer> fsdClustersLinked = null;
        double fsdAccuracy;
        
        //Running FSD algorithm on tweets and exporting clustering
        FSDBuilder.runFSDClustering(tweetsUniqueTerms, tweetsFreqMatrix);
        
        //Importing clusterings
        fsdClustering = importClustering(FSDBuilder.FSD_CLUSTERING_FILE_PATH);
        //Building frequency matrix
        fsdClustersFreqMatrix = getClustersFrequencyMatrix(fsdClustering, tweetsFreqMatrix);
        //Linking real clusters themes with the algorithm clusters using the cosine similarity
        fsdClustersLinked = linkRealClustersWithCorrectTweets(realClustering, fsdClustersLinked, fsdClustering);
        //Calculating clustering algorithm accuracy
        fsdAccuracy = getClusteringAccuracy(realClustering, fsdClustersLinked, fsdClustering);
        System.out.println(String.format("FSD algorithm :\nAccuracy : %f\nClusters : %d", fsdAccuracy, fsdClustering.size()));
    }
    
    /**
     * F-score algorithm for evaluating clustering algorithms
     * A cluster score is :
     * F-score = (P + R) / (2PR)
     * With P = precision = number of CORRECT tweets in the NEW cluster / TOTAL number of tweets in the NEW cluster
     * And R = recall = number of CORRECT tweets in the NEW cluster / TOTAL number of tweets in the REAL cluster
     * So that, a clustering F-score is the average of every cluster F-score
     * @param realClustering The real (correct) clustering
     * @param newClusteringLinked For each new cluster ID, which real cluster ID is it linked with
     * @param newClustering The new cluster to evaluate
     * @return The F-score value (the highest one is the best)
     */
    private static double getClusteringAccuracy(HashMap<Integer, List<String>> realClustering, HashMap<Integer, Integer> newClusteringLinked, HashMap<Integer, List<String>> newClustering)
    {
        System.out.println("Calculating cluster accuracy with F-score...");
        List<String> correctTweets, realClusterTweets, newClusterTweets;
        List<Double> clustersAccuracy = new ArrayList<Double>();
        int newClustersNumber = newClustering.size();
        double clusteringAccuracy = 0;
        double clusterAccuracy;
        
        for(Integer newClusterId : newClustering.keySet())
        {
            realClusterTweets = realClustering.get(newClusteringLinked.get(newClusterId));
            newClusterTweets = newClustering.get(newClusterId);
            correctTweets = getIntersection(realClusterTweets, newClusterTweets);
            
            clusterAccuracy = calculateFScore(newClusterTweets.size(), realClusterTweets.size(), correctTweets.size());
            clusteringAccuracy += clusterAccuracy / newClustersNumber;
        }
        
        System.out.println("Done");
        
        return clusteringAccuracy;
    }
    
    /**
     * Returns the F-score based on the given parameters
     * F-score = (P + R) / (2PR)
     * With P = precision = number of CORRECT tweets in the NEW cluster / TOTAL number of tweets in the NEW cluster
     * And R = recall = number of CORRECT tweets in the NEW cluster / TOTAL number of tweets in the REAL cluster
     * @param nbTweetsNewClusters The number of tweets of the new cluster
     * @param nbTweetsRealClusters The number of tweets of the real cluster
     * @param nbCorrecTweets The number of tweets in the new cluster which are also in the real cluster
     * @return Returns the F-score based on the given parameters
     */
    private static double calculateFScore(int nbTweetsNewClusters, int nbTweetsRealClusters, int nbCorrecTweets)
    {
        if(nbCorrecTweets == 0)
        {
            return 0;
        }
        double precision = nbCorrecTweets / (double) nbTweetsNewClusters;
        double recall = nbCorrecTweets / (double) nbTweetsRealClusters;
        return (2 * precision * recall) / (precision + recall);
    }
    
    /**
     * Returns the intersection of two lists
     * @param list1 The first list
     * @param list2 The second list
     * @return The intersection of the lists
     */
    private static List<String> getIntersection(List<String> list1, List<String> list2)
    {
        List<String> list = new ArrayList<String>();

        for (String t : list1)
        {
            if(list2.contains(t))
            {
                list.add(t);
            }
        }

        return list;
    }
    
    private static HashMap<Integer, Integer> linkRealClustersWithCorrectTweets(HashMap<Integer, List<String>> realClustering, HashMap<Integer, Integer> newClusteringLinked, HashMap<Integer, List<String>> newClustering)
    {
        System.out.println("Linking real clusters with calculated clusters...");
        
        List<String> correctTweets, realClusterTweets, newClusterTweets;
        HashMap<Integer, Integer> clustersLinked = new HashMap<Integer, Integer>();
        int correctTweetsNumber, tmpCorrectTweetsNumber;
        Integer bestClusterId = null;
        
        for(Integer newClusterId : newClustering.keySet())
        {
            correctTweetsNumber = 0;
            newClusterTweets = newClustering.get(newClusterId);
            
            for(Integer realClusterId : realClustering.keySet())
            {
                realClusterTweets = realClustering.get(realClusterId);
                correctTweets = getIntersection(realClusterTweets, newClusterTweets);
                tmpCorrectTweetsNumber = correctTweets.size();
                
                if(tmpCorrectTweetsNumber > correctTweetsNumber)
                {
                    bestClusterId = realClusterId;
                }
            }
            
            clustersLinked.put(newClusterId, bestClusterId);
        }
        
        System.out.println("Done");
        
        return clustersLinked;
    }
    
    /**
     * DO NOT USE => BUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUGS !
     * Links calculated clusters to the real clusters,
     * basing on the cosine similarity between the term vector of the
     * real cluster theme, and the term vector of the calculated cluster
     * @param realClustersThemesFreqMatrix The frequency matrix (many terms vectors) of the real clusters themes
     * @param realClustersUniqueTerms The set containing every (unique) term in the real clusters themes
     * @param newClustersFreqMatrix The frequency matrix (many terms vectors) of the calculated cluster to link
     * @return 
     */
    private static HashMap<Integer, Integer> linkRealClustersWithCosSimilarity(HashMap<String, HashMap<String, Long>> realClustersThemesFreqMatrix, HashSet<String> realClustersUniqueTerms, HashMap<Integer, HashMap<String, Long>> newClustersFreqMatrix)
    {
        HashMap<Integer, Integer> clustersLinked = new HashMap<Integer, Integer>();
        double minCos, tmpCos;
        Integer bestCluster = null;
        
        System.out.println("Linking real clusters with calculated clusters...");
        
        //Browsing each calculated cluster ID
        for(Integer newClusterId : newClustersFreqMatrix.keySet())
        {
            HashMap<String, Long> newTermsVector = newClustersFreqMatrix.get(newClusterId);
            Long[] newClusterFreqVector = FSDBuilder.getFrequencyVector(realClustersUniqueTerms, newTermsVector);
            minCos = -1;
            
            //Browsing the real clusters ID
            for(String realClusterId : realClustersThemesFreqMatrix.keySet())
            {
                HashMap<String, Long> realTermsVector = realClustersThemesFreqMatrix.get(realClusterId);
                Long[] realClusterFreqVector = FSDBuilder.getFrequencyVector(realClustersUniqueTerms, realTermsVector);
                
                tmpCos = FSDBuilder.getCosineSimilarity(newClusterFreqVector, realClusterFreqVector);
                if(tmpCos > minCos)
                {
                    minCos = tmpCos;
                    bestCluster = Integer.parseInt(realClusterId);
                }
            }
            
            //Assigning the closest (regarding to cosine similarity) real cluster ID to a calculated cluster ID
            clustersLinked.put(newClusterId, bestCluster);
        }
        
        System.out.println("Done");
        
        return clustersLinked;
    }
    
    /**
     * Builds a frequency matrix containing a terms vector for each cluster, sum of tweets terms vector contained by the current cluster
     * @param clustering The clustering to study
     * @param tweetsFrequencyMatrix The terms vector of every tweets in the clusters
     * @return The clusters frequency matrix
     */
    private static HashMap<Integer, HashMap<String, Long>> getClustersFrequencyMatrix(HashMap<Integer, List<String>> clustering, HashMap<String, HashMap<String, Long>> tweetsFrequencyMatrix)
    {
        System.out.println("Building cluster frequency matrix...");
        HashMap<Integer, HashMap<String, Long>> clusterFrequencyMatrix = new HashMap<Integer, HashMap<String, Long>>();
        Long occurrences;
        
        //Browsing each cluster
        for(Integer clusterId : clustering.keySet())
        {
            HashMap<String, Long> clusterTermsVector = new HashMap<String, Long>();
            
            //Browsing each tweet in the cluster
            for(String tweetId : tweetsFrequencyMatrix.keySet())
            {
                //Adding the tweet terms vector to the cluster one
                HashMap<String, Long> tweetsTermVector = tweetsFrequencyMatrix.get(tweetId);
                
                //Browsing the terms of the current tweets
                for(String term : tweetsTermVector.keySet())
                {
                    //Adding the term occurrences to the clusters frequency matrix
                    occurrences = clusterTermsVector.get(term);
                    if(occurrences == null)
                    {
                        clusterTermsVector.put(term, tweetsTermVector.get(term));
                    }
                    else
                    {
                        clusterTermsVector.put(term, occurrences + tweetsTermVector.get(term));
                    }
                }
            }
            
            clusterFrequencyMatrix.put(clusterId, clusterTermsVector);
        }
        
        System.out.println("Done");
        
        return clusterFrequencyMatrix;
    }
    
    /**
     * Import the clusters themes
     * @return For each cluster, its theme (a string)
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private static HashMap<Integer, String> importClusterThemes() throws FileNotFoundException, IOException
    {
        System.out.println("Importing clusters themes...");
        String line;
        String[] array;
        HashMap<Integer, String> clustersThemes = new HashMap<Integer, String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(CLUSTERS_THEMES_FILE_PATH)));
        
        while((line = reader.readLine()) != null)
        {
            array = line.split("#");
            clustersThemes.put(Integer.parseInt(array[0]), array[1]);
        }
        
        reader.close();
        System.out.println("Done");
        
        return clustersThemes;
    }
    
    /**
     * From the clusters themes, returns the same data with a structure compatible with the matrix frequency builder algorithm
     * @param themes The clusters themes
     * @return The clusters themes compatible with the matrix frequency builder algorithm
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private static List<HashMap<String, String>> getMatrixFrequencyCompatibleThemes(HashMap<Integer, String> themes) throws FileNotFoundException, IOException
    {
        System.out.println("Transforming cluster themes to matrix frequency compatibles...");
        List<HashMap<String, String>> clustersThemes = new ArrayList<HashMap<String, String>>();
        
        for(Integer clusterId : themes.keySet())
        {
            HashMap<String, String> theme = new HashMap<String, String>();
            theme.put(FrequencyMatrixBuilder.ID, clusterId.toString());
            theme.put(FrequencyMatrixBuilder.TEXT, themes.get(clusterId));
            clustersThemes.add(theme);
        }
        
        System.out.println("Done");
        
        return clustersThemes;
    }
    
    /**
     * Import a clustering from a file
     * @param filePath The file to parse
     * @return The importer clustering
     * @throws IOException 
     */
    private static HashMap<Integer, List<String>> importClustering(String filePath) throws IOException
    {
        System.out.println("Importing clustering...");
        String line;
        Integer clusterId;
        List<String> tweetsList;
        String[] array;
        HashMap<Integer, List<String>> clustering = new HashMap<Integer, List<String>>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        
        while((line = reader.readLine()) != null)
        {
            array = line.split(" ");
            clusterId = Integer.parseInt(array[0]);
            tweetsList = clustering.get(clusterId);
            if(tweetsList == null)
            {
                tweetsList = new ArrayList();
                clustering.put(clusterId, tweetsList);
            }
            tweetsList.add(array[1]);
        }
        
        reader.close();
        System.out.println("Done");
        
        return clustering;
    }
}
