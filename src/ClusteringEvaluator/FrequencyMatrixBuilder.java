/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ClusteringEvaluator;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

/**
 *
 */
public class FrequencyMatrixBuilder {

    /**
     * Frequency matrix file paths
     */
    private static final String FREQUENCY_MATRIX_FILE_PATH = "results\\frequencyMatrix.csv";
    /**
     * Database configuration
     */
    private static final String DATABASE_DRIVER = "jdbc:sqlite";
    private static final String DATABASE_FILE_PATH = "resources\\tweets.db";
    private static final String TWEETS_DATABASE = String.format("%s:%s", DATABASE_DRIVER, DATABASE_FILE_PATH);
    public static final String ID = "id";
    public static final String TEXT = "text";

    /**
     * Builds the terms matrix associated to the tweets corpus
     *
     * @param freqMatrix
     * @param uniqueTerms
     * @param tuples
     * @throws IOException
     */
    public static void getTermsFrequencyMatrix(HashMap<String, HashMap<String, Long>> freqMatrix, HashSet<String> uniqueTerms, List<HashMap<String, String>> tuples) throws IOException {
        System.out.println("Building frequency matrix...");

        // 0. Specify the analyzer for tokenizing text.
        //    The same analyzer should be used for indexing and searching
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);

        // 1. create the index
        Directory index = new RAMDirectory();

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_44, analyzer);

        IndexWriter w = new IndexWriter(index, config);
        if (tuples == null) {
            addDocsWithDB(w);
        } else {
            for (HashMap<String, String> tuple : tuples) {
                addDoc(tuple, w);
            }
        }
        w.commit();
        w.close();

        IndexReader reader = DirectoryReader.open(index);

        for (int i = 0; i < reader.numDocs(); i++) {
            Document doc = reader.document(i);
            String id = doc.get(ID);
            Terms vector = reader.getTermVector(i, TEXT);
            if (vector != null) {
                TermsEnum termsEnum = null;
                termsEnum = vector.iterator(termsEnum);
                HashMap<String, Long> frequencies = new HashMap<String, Long>();
                BytesRef text = null;
                ArrayList<String> terms = new ArrayList<String>();
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();
                    long freq = (long) termsEnum.totalTermFreq();
                    frequencies.put(term, freq);
                    terms.add(term);
                    uniqueTerms.add(term);
                }
                freqMatrix.put(id, frequencies);
            }
        }
        reader.close();
        System.out.println("Done");
    }

    /**
     * Fetch tweets from database
     *
     * @param w
     * @throws IOException
     */
    private static void addDocsWithDB(IndexWriter w) throws IOException {
        try {
            try {
                DBManager.ConnectToDB(TWEETS_DATABASE, 30);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(FSDBuilder.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SQLException ex) {
                Logger.getLogger(FSDBuilder.class.getName()).log(Level.SEVERE, null, ex);
            }

            Statement statement = DBManager.connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery("SELECT * FROM tweets");
            long count = 0;

            while (rs.next()) {
                HashMap<String, String> tuple = new HashMap<String, String>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String f = rs.getString(i) == null ? "" : rs.getString(i);
                    tuple.put(rs.getMetaData().getColumnLabel(i), f);
                }
                addDoc(tuple, w);
                if (count % 100 == 0) {
                    System.out.println(count);
                }
                count++;
            }
        } catch (SQLException ex) {
            Logger.getLogger(FSDBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Add a single document to the index
     *
     * @param tuple
     * @param w
     * @throws IOException
     */
    private static void addDoc(HashMap<String, String> tuple, IndexWriter w) throws IOException {
        Document doc = new Document();
        for (String key : tuple.keySet()) {
            FieldType type = new FieldType();
            type.setIndexed(true);
            type.setStored(true);
            type.setStoreTermVectors(true);
            Field field = new Field(key, tuple.get(key), type);
            doc.add(field);
        }
        w.addDocument(doc);
    }

    /**
     * Exports frequuency matrix to CSV file
     *
     * @param freqMatrix
     * @param uniqueTerms
     * @throws IOException
     */
    public static void exportFrequencyMatrix(HashMap<String, HashMap<String, Long>> freqMatrix, HashSet<String> uniqueTerms) throws IOException {
        System.out.println("Exporting frequency matrix...");
        FileWriter writer = new FileWriter(FREQUENCY_MATRIX_FILE_PATH);

        writer.append("\"id_tweet\",");
        Iterator it = uniqueTerms.iterator();
        while (it.hasNext()) {
            String term = (String) it.next();
            writer.append("\"");
            writer.append(term);
            writer.append("\"");
            if (it.hasNext()) {
                writer.append(",");
            }
        }

        writer.append("\n");

        for (String key : freqMatrix.keySet()) {
            HashMap<String, Long> vector = freqMatrix.get(key);
            Iterator itt = uniqueTerms.iterator();
            writer.append("\"");
            writer.append(key);
            writer.append("\"");
            writer.append(",");
            while (itt.hasNext()) {
                String term = (String) itt.next();
                writer.append("\"");
                if (vector.get(term) == null) {
                    writer.append("0");
                } else {
                    writer.append(String.valueOf(vector.get(term)));
                }
                writer.append("\"");

                if (itt.hasNext()) {
                    writer.append(",");
                }
            }
            writer.append("\n");
        }
        writer.flush();
        writer.close();
        System.out.println("Done");
    }
}
