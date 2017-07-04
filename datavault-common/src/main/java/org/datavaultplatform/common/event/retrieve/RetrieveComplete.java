package org.datavaultplatform.common.event.retrieve;

import javax.persistence.Entity;

import org.datavaultplatform.common.event.Event;

@Entity
public class RetrieveComplete extends Event {

    RetrieveComplete() {};
    public RetrieveComplete(String jobId, String depositId, String retrieveId) {
        super(jobId, depositId, retrieveId, "Deposit retrieve completed");
        this.eventClass = RetrieveComplete.class.getCanonicalName();
    }
}
