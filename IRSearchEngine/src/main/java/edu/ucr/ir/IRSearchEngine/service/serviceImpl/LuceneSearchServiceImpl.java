package edu.ucr.ir.IRSearchEngine.service.serviceImpl;

import edu.ucr.ir.IRSearchEngine.model.SearchResultEntity;
import edu.ucr.ir.IRSearchEngine.service.LuceneSearchService;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class LuceneSearchServiceImpl implements LuceneSearchService {

    @Override
    public List<SearchResultEntity> getSearchResults(String queryString) throws IOException, ParseException {
        String index = "C:\\Users\\soura\\Documents\\IntellijIdea\\wrkSpace1\\IRProject\\src\\Resources\\Output\\Lucene";
        String field = "contents";
        int hitsPerPage = 20;
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser(field, analyzer);
        Query query = parser.parse(queryString);
        searcher.search(query, 100);
        TopDocs results = searcher.search(query, 5 * hitsPerPage);
        ScoreDoc[] hits = results.scoreDocs;
        int numTotalHits = (int)results.totalHits.value;
        int end = Math.min(numTotalHits, hitsPerPage);
        List<SearchResultEntity> resList = new ArrayList<>();
        for (int i = 0; i < end; i++) {
            Document doc = searcher.doc(hits[i].doc);
            String url = doc.get("url");
            String heading = doc.get("heading");
            SearchResultEntity searchResultEntity = new SearchResultEntity(i+1, heading, url );
            resList.add(searchResultEntity);
        }
        reader.close();
        return  resList;
    }
}
