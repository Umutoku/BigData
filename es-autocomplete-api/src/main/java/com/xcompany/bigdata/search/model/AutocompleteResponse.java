package com.xcompany.bigdata.search.model;

import java.util.List;

public class AutocompleteResponse {
    private List<AutoCompleteDetail> data;

    public AutocompleteResponse() {

    }

    public AutocompleteResponse(List<AutoCompleteDetail> data) {
        this.data = data;
    }



    public List<AutoCompleteDetail> getData() {
        return data;
    }

    public void setData(List<AutoCompleteDetail> data) {
        this.data = data;
    }
}
