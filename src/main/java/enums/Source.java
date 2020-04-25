package enums;

public enum Source {

    BIORXIV("biorxiv_medrxiv/biorxiv_medrxiv"),
    COMM_USE("comm_use_subset/comm_use_subset"),
    CUSTOM_LICENSE("custom_license/custom_license"),
    NON_COMM_USE("noncomm_use_subset/noncomm_use_subset");


    private String path;

    Source(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
