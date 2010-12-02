/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;

@Entity
@IdClass(BundleActionId.class)
@Table(name = "BUNDLE_ACTIONS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
@NamedQueries( {
        @NamedQuery(name = "DELETE_BUNDLE_ACTION", query = "delete from BundleActionBean w where w.bundleId = :bundleId AND w.coordName = :coordName"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS", query = "select OBJECT(w) from BundleActionBean w where w.bundleId = :bundleId"),

        @NamedQuery(name = "GET_BUNDLE_ACTION", query = "select OBJECT(w) from BundleActionBean w where w.bundleId = :bundleId AND w.coordName = :coordName"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_COUNT", query = "select count(w) from BundleActionBean w"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_OLDER_THAN", query = "select OBJECT(w) from BundleActionBean w order by w.lastModifiedTimestamp") })
public class BundleActionBean implements Writable {

    @Id
    @Column(name = "bundle_id")
    private String bundleId = null;

    @Id
    @Column(name = "coord_name")
    private String coordName = null;

    @Basic
    @Column(name = "coord_id")
    private String coordId = null;

    @Basic
    @Column(name = "status")
    private String status = null;

    @Basic
    @Column(name = "critical")
    private int critical = 0;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    @Basic
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    /**
     * @return bundleId
     */
    public String getBundleId() {
        return bundleId;
    }

    /**
     * @param bundleId
     */
    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    /**
     * @return coordName
     */
    public String getCoordName() {
        return coordName;
    }

    /**
     * @param coordName
     */
    public void setCoordName(String coordName) {
        this.coordName = coordName;
    }

    /**
     * @return the coordId
     */
    public String getCoordId() {
        return coordId;
    }

    /**
     * @param coordId
     */
    public void setCoordId(String coordId) {
        this.coordId = coordId;
    }

    /**
     * @return status object
     */
    public Status getStatus() {
        return Status.valueOf(this.status);
    }

    /**
     * @return status string
     */
    public String getStatusStr() {
        return status;
    }

    /**
     * @param val
     */
    public void setStatus(Status val) {
        this.status = val.toString();
    }

    /**
     * @param critical set critical to true
     */
    public void setCritical() {
        this.critical = 1;
    }

    /**
     * @param critical set critical to false
     */
    public void resetCritical() {
        this.critical = 0;
    }

    /**
     * Return if the action is critical.
     *
     * @return if the action is critical.
     */
    public boolean isCritical() {
        return critical == 1 ? true : false;
    }

    /**
     * @param pending set pending to true
     */
    public void setPending() {
        this.pending = 1;
    }

    /**
     * @param pending set pending to false
     */
    public void resetPending() {
        this.pending = 0;
    }

    /**
     * Return if the action is pending.
     *
     * @return if the action is pending.
     */
    public boolean isPending() {
        return pending == 1 ? true : false;
    }

    /**
     * @param lastModifiedTimestamp the lastModifiedTimestamp to set
     */
    public void setLastModifiedTimestamp(java.sql.Timestamp lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    /**
     * @param lastModifiedTime the lastModifiedTime to set
     */
    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    /**
     * @return lastModifiedTime
     */
    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    /**
     * @return lastModifiedTimestamp
     */
    public Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getBundleId());
        WritableUtils.writeStr(dataOutput, getCoordName());
        WritableUtils.writeStr(dataOutput, getCoordId());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        dataOutput.writeInt(critical);
        dataOutput.writeInt(pending);
        dataOutput.writeLong((getLastModifiedTimestamp() != null) ? getLastModifiedTimestamp().getTime() : -1);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setBundleId(WritableUtils.readStr(dataInput));
        setCoordName(WritableUtils.readStr(dataInput));
        setCoordId(WritableUtils.readStr(dataInput));
        setStatus(Status.valueOf(WritableUtils.readStr(dataInput)));
        critical = dataInput.readInt();
        pending = dataInput.readInt();
        long d = dataInput.readLong();
        if (d != -1) {
            setLastModifiedTime(new Date(d));
        }
    }

}
