package model;

import java.util.List;

public class CovidMeta {

    private String id;
    private String paperId;
    private String sha;
    private String title;
    private List<String> textAbstract;
    private List<String> authors; //full name of authors
    private List<String> bodyText; //paragraphs
    private String publishTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSha() {
        return sha;
    }

    public void setSha(String sha) {
        this.sha = sha;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getTextAbstract() {
        return textAbstract;
    }

    public void setTextAbstract(List<String> textAbstract) {
        this.textAbstract = textAbstract;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    public String getPublishTime() {
        return publishTime;
    }

    public void setPublishTime(String publishTime) {
        this.publishTime = publishTime;
    }

    public List<String> getBodyText() {
        return bodyText;
    }

    public void setBodyText(List<String> bodyText) {
        this.bodyText = bodyText;
    }

    public String getPaperId() {
        return paperId;
    }

    public void setPaperId(String paperId) {
        this.paperId = paperId;
    }

    @Override
    public String toString() {
        return "CovidMeta{" +
                "id='" + id + '\'' +
                ", paperId='" + paperId + '\'' +
                ", sha='" + sha + '\'' +
                ", title='" + title + '\'' +
                ", textAbstract=" + textAbstract +
                ", authors=" + authors +
                ", bodyText=" + bodyText +
                ", publishTime='" + publishTime + '\'' +
                '}';
    }
}
