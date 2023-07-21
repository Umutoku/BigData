public class SearchProductModel  {
    private String prodcut;
    private String time;

    public SearchProductModel(String prodcut, String time) {
        this.prodcut = prodcut;
        this.time = time;
    }

    public String getProdcut() {
        return prodcut;
    }

    public void setProdcut(String prodcut) {
        this.prodcut = prodcut;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public SearchProductModel(){


    }


}
