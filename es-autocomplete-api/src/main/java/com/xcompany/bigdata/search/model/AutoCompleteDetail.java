package com.xcompany.bigdata.search.model;

public class AutoCompleteDetail {
    private String brand;
    private String title;

    public AutoCompleteDetail(String brand, String title) {
        this.brand = brand;
        this.title = title;
    }

    public AutoCompleteDetail() {
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
