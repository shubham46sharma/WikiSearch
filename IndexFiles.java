package Lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;

import java.io.*;



public class IndexFiles {

    private IndexFiles() {}

    public static void main(String[] args) {

//        String indexPath = "/opt/home/cs242-w22/files/Output";
//        String docsPath = "/opt/home/cs242-w22/dataset";
        String indexPath = "./src/Resources/Output/Lucene";
        String docsPath = "./src/Resources/Input/CrawlData";
        boolean create = true;

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }
            //IndexWriter to write the index to the documents.
            IndexWriter writer = new IndexWriter(dir, iwc);
            //indexDocs function is called for indexing.
            indexDocs(writer, docDir);

            writer.close();
            //To calculate the total run time of the algorithm
            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() +
                    "\n with message: " + e.getMessage());
        }
    }


    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            Document doc = new Document();

            // Add the path of the file as a field named "path".
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);
            Reader rf = new InputStreamReader(stream);
            BufferedReader bf = new BufferedReader(rf);
            String link = bf.readLine();
            System.out.println(link);
            Field url = new StringField("url",link, Field.Store.YES);
            doc.add(url);
            String name =  bf.readLine();
            Field heading = new StringField("heading", name, Field.Store.YES);
            doc.add(heading);

            // Add the contents of the file to a field named "contents".  Created a Reader,
            // so that the text of the file is tokenized and indexed, but not stored.
            // Note that FileReader expects the file to be in UTF-8 encoding.
            // If that's not the case searching for special characters will fail.
            doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }
}



//
//public class IndexFiles {
//
//    private IndexFiles() {}
//
//    public static void main(String[] args) {
//        String indexPath = "./src/Resources/Output/Lucene";
//        String docsPath = "./src/Resources/Input/CrawlData";
//
//        boolean create = true;
//
//        final Path docDir = Paths.get(docsPath);
//        if (!Files.isReadable(docDir)) {
//            System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
//            System.exit(1);
//        }
//
//        Date start = new Date();
//        try {
//            System.out.println("Indexing to directory '" + indexPath + "'...");
//
//            Directory dir = FSDirectory.open(Paths.get(indexPath));
//            Analyzer analyzer = new StandardAnalyzer();
//            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
//
//            if (create) {
//                // Create a new index in the directory, removing any
//                // previously indexed documents:
//                iwc.setOpenMode(OpenMode.CREATE);
//            } else {
//                // Add new documents to an existing index:
//                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
//            }
//            //IndexWriter to write the index to the documents.
//            IndexWriter writer = new IndexWriter(dir, iwc);
//            //indexDocs function is called for indexing.
//            indexDocs(writer, docDir);
//
//            writer.close();
//            //To calculate the total run time of the algorithm
//            Date end = new Date();
//            System.out.println(end.getTime() - start.getTime() + " total milliseconds");
//
//        } catch (IOException e) {
//            System.out.println(" caught a " + e.getClass() +
//                    "\n with message: " + e.getMessage());
//        }
//    }
//
//
//    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
//        if (Files.isDirectory(path)) {
//            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
//                @Override
//                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
//                    try {
//                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
//                    } catch (IOException ignore) {
//                        // don't index files that can't be read.
//                    }
//                    return FileVisitResult.CONTINUE;
//                }
//            });
//        } else {
//            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
//        }
//    }
//
//    /** Indexes a single document */
//    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
//        try (InputStream stream = Files.newInputStream(file)) {
//            // make a new, empty document
//            Document doc = new Document();
//
//            // Add the path of the file as a field named "path".
//            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
//            doc.add(pathField);
//
//
//            // Add the contents of the file to a field named "contents".  Created a Reader,
//            // so that the text of the file is tokenized and indexed, but not stored.
//            // Note that FileReader expects the file to be in UTF-8 encoding.
//            // If that's not the case searching for special characters will fail.
//            doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
//
//            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
//                // New index, so we just add the document (no old document can be there):
//                System.out.println("adding " + file);
//                writer.addDocument(doc);
//            } else {
//                // Existing index (an old copy of this document may have been indexed) so
//                // we use updateDocument instead to replace the old one matching the exact
//                // path, if present:
//                System.out.println("updating " + file);
//                writer.updateDocument(new Term("path", file.toString()), doc);
//            }
//        }
//    }
//}
//
