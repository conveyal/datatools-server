package com.conveyal.datatools.editor.models.transit;

import com.conveyal.datatools.editor.models.Model;
import com.conveyal.gtfs.model.FareRule;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Created by landon on 6/22/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class Fare extends Model implements Cloneable, Serializable {
    public static final long serialVersionUID = 1;

    public String feedId;
    public String gtfsFareId;
    public String description;
    public Double price;
    public String currencyType;
    public Integer paymentMethod;
    public Integer transfers;
    public Integer transferDuration;
    public List<FareRule> fareRules  = Lists.newArrayList();

    public Fare() {}

    public Fare(com.conveyal.gtfs.model.FareAttribute fare, List<com.conveyal.gtfs.model.FareRule> rules, EditorFeed feed) {
        this.gtfsFareId = fare.fare_id;
        this.price = fare.price;
        this.currencyType = fare.currency_type;
        this.paymentMethod = fare.payment_method;
        this.transfers = fare.transfers;
        this.transferDuration = fare.transfer_duration;
        this.fareRules.addAll(rules);
        this.feedId = feed.id;
        inferName();
    }

    /**
     * Infer the name of this calendar
     */
    public void inferName () {
        StringBuilder sb = new StringBuilder(14);

        if (price != null)
            sb.append(price);
        if (currencyType != null)
            sb.append(currencyType);

        this.description = sb.toString();

        if (this.description.equals("") && this.gtfsFareId != null)
            this.description = gtfsFareId;
    }

    public Fare clone () throws CloneNotSupportedException {
        Fare f = (Fare) super.clone();
        f.fareRules.addAll(fareRules);
        return f;
    }

    public com.conveyal.gtfs.model.Fare toGtfs() {
        com.conveyal.gtfs.model.Fare fare = new com.conveyal.gtfs.model.Fare(this.gtfsFareId);
        fare.fare_attribute = new com.conveyal.gtfs.model.FareAttribute();
        fare.fare_attribute.fare_id = this.gtfsFareId;
        fare.fare_attribute.price = this.price == null ? Double.NaN : this.price;
        fare.fare_attribute.currency_type = this.currencyType;
        fare.fare_attribute.payment_method = this.paymentMethod == null ? Integer.MIN_VALUE : this.paymentMethod;
        fare.fare_attribute.transfers = this.transfers == null ? Integer.MIN_VALUE : this.transfers;
        fare.fare_attribute.transfer_duration = this.transferDuration == null ? Integer.MIN_VALUE : this.transferDuration;
        fare.fare_attribute.feed_id = this.feedId;

        fare.fare_rules.addAll(this.fareRules);
        return fare;
    }
}
