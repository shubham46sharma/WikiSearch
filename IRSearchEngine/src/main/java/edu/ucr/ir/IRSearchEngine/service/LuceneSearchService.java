package edu.ucr.ir.IRSearchEngine.service;

import edu.ucr.ir.IRSearchEngine.model.SearchResultEntity;
import org.springframework.stereotype.Service;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.List;


@Service
public interface LuceneSearchService {
    List<SearchResultEntity> getSearchResults(String queryString) throws IOException, ParseException;
}
