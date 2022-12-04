package edu.ucr.ir.IRSearchEngine.model;

public class SearchResultEntity {

    private int index;
    private String heading;
    private String url;

    public SearchResultEntity(int searchTerm, String heading, String url) {
        this.index = searchTerm;
        this.heading = heading;
        this.url = url;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getHeading() {
        return heading;
    }

    public void setHeading(String heading) {
        this.heading = heading;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
