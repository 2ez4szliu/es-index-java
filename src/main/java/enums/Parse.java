package enums;

public enum Parse {
    PDF("pdf_json"),
    PMC("pmc_json");

    private String path;
    Parse(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
